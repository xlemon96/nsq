package nsqd

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"

	"nsq/internal/lg"
	"nsq/internal/pqueue"
	"nsq/internal/quantile"
)

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}
//channel是消费者可订阅的最小单元，消息流将会按照如何形式转发
//一个消费者可以订阅一个topic，并且只能订阅一次，不可重复订阅(通过subeventchan控制，订阅完成后关闭此chan)
//topic->channel->consumer1,consumer2,consumer3
type Channel struct {
	//全局锁
	sync.RWMutex
	//状态值，客户端要求重新投递消息数量、接受topic发送过来的消息数量、超时消息数量
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64
	//topic名称，channel名称，全局context(包含nsqd结构体)
	topicName string
	name      string
	ctx       *context
    //持久化队列，目前为diskquene
	backend BackendQueue
	//内存消息chan，存放在内存中的message
	memoryMsgChan chan *Message
	//退出标志，退出锁
	exitFlag      int32
	exitMutex     sync.RWMutex
	//所有订阅此channel的消费者，对应一个client实例
	clients        map[int64]Consumer
	//暂定标志
	paused         int32
	//是否临时channel
	ephemeral      bool
	//删除此channel的清理函数(只有当订阅此chan的消费者数量为0，且此channel为临时channel才会调用)
	deleteCallback func(*Channel)
	//保证清理函数只执行一次
	deleter        sync.Once
	//----------------------
	//---------未知----------
	//----------------------
	e2eProcessingLatencyStream *quantile.Quantile
	//延迟消息队列，map(方便快速通过messageID快速找到message)，队列锁
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue
	deferredMutex    sync.Mutex
	//待确认消息队列，map(方便快速通过messageID快速找到message)，队列锁
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex
}
//新创建一个channel
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {
	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  nil,
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}
	//赋值内存最大存放消息数量
	if ctx.nsqd.getOpts().MemQueueSize > 0 {
		c.memoryMsgChan = make(chan *Message, ctx.nsqd.getOpts().MemQueueSize)
	}
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}
	//初始化消息队列
	c.initPQ()
	//若是临时节点，则持久化队列操作为空
	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		//设置持久化消息队列名称
		backendName := getBackendName(topicName, channelName)
		c.backend = diskqueue.New(
			backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}
	//通知nsqd，channel创建完成
	c.ctx.nsqd.Notify(c)
	return c
}
//初始化一个消息队列
//队列长度为内存消息数量的1/10
func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.getOpts().MemQueueSize)/10))
	c.inFlightMutex.Lock()
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()
	c.deferredMutex.Lock()
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}
//退出channel，置exit标志位为1
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}
//删除channel，exiting为系统正常退出，此函数为删除一个channel并清空此channel数据
func (c *Channel) Delete() error {
	return c.exit(true)
}
//关闭此channel
func (c *Channel) Close() error {
	return c.exit(false)
}
//退出函数，包含删除和正常退出两种情况
func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()
	//通过cas可以判断此时channel是否已经退出，若已经退出，则直接return
	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}
	//若为true，则通知nsqd删除channel
	if deleted {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): deleting", c.name)
		c.ctx.nsqd.Notify(c)
	} else {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing", c.name)
	}
	c.RLock()
	//关闭所有消费者
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()
	//若为true，则清空此channel数据
	if deleted {
		c.Empty()
		return c.backend.Delete()
	}
	//若关闭，则数据刷盘
	c.flush()
	return c.backend.Close()
}
//清空channel数据
func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()
	//通过初始化消息队列清空待确认、延迟发送消息
	c.initPQ()
	//清空client相关状态
	for _, client := range c.clients {
		client.Empty()
	}
	for {
		select {
		//清空内存中的消息
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}
finish:
	//清空持久化队列消息
	return c.backend.Empty()
}
//将此channel数据刷盘，通过将所有内存中的数据发送到持久化消息队列
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer
	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}
	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}
finish:
	c.inFlightMutex.Lock()
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()
	c.deferredMutex.Lock()
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.deferredMutex.Unlock()
	return nil
}
//返回有多少未发送的消息
//包含内存数据+磁盘数据
func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}
//暂停此channel
func (c *Channel) Pause() error {
	return c.doPause(true)
}
//恢复cichannel
func (c *Channel) UnPause() error {
	return c.doPause(false)
}
//暂停恢复执行者
func (c *Channel) doPause(pause bool) error {
	//更新标志位
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}
	c.RLock()
	//暂定或恢复所有消费者
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}
//是否已经暂停
func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}
//放入一个消息到此channel
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	if c.Exiting() {
		return errors.New("exiting")
	}
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}
//放入一个消息到此channel
func (c *Channel) put(m *Message) error {
	select {
	//若可以放入到memoryMsgChan，则内存未满，放入内存
	case c.memoryMsgChan <- m:
	default:
		b := bufferPoolGet()
		err := writeMessageToBackend(b, m, c.backend)
		bufferPoolPut(b)
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}
//放入一个延迟消息
func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1)
	c.StartDeferredTimeout(msg, timeout)
}
//表示客户端希望重置消息的超时时间
//从待确认的消息队列中取出消息，删除map中的消息
//设置新超时时间，并重新放入待确认消息队列，map(重新发送)
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.getOpts().MaxMsgTimeout {
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}
	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}
//消息发送成功，客户端返回fin报文
//从待确认的消息队列及map中移除此message
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}
//客户端发送req报文，希望消息重新被发送
//若timeoutMs为0，则重新发送；若timeoutMs>0，则延迟timeoutMs发送，加入延迟消息队列
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	atomic.AddUint64(&c.requeueCount, 1)
	//若timeout等于0，则调用put函数将消息放入memchan或者diskquene，等待发送
	if timeout == 0 {
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		err := c.put(msg)
		c.exitMutex.RUnlock()
		return err
	}
	//否则进入加入到延迟消息队列流程
	return c.StartDeferredTimeout(msg, timeout)
}
//增加一个消费者到此channel
func (c *Channel) AddClient(clientID int64, client Consumer) error {
	c.Lock()
	defer c.Unlock()
	_, ok := c.clients[clientID]
	if ok {
		return nil
	}
	//超过channel能够接受的消费者数量，直接返回error
	maxChannelConsumers := c.ctx.nsqd.getOpts().MaxChannelConsumers
	if maxChannelConsumers != 0 && len(c.clients) >= maxChannelConsumers {
		return errors.New("E_TOO_MANY_CHANNEL_CONSUMERS")
	}
	c.clients[clientID] = client
	return nil
}
//删除一个client
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()
	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)
	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}
//将一个消息放入消息队列，并附带超时时间
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}
//将消息放入延迟消息队列
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()
	item := &pqueue.Item{Value: msg, Priority: absTs}
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item)
	return nil
}
//两个队列的增加、删除函数
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}
func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}
func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}
func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}
func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}
func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}
//处理延时消息队列
func (c *Channel) processDeferredQueue(t int64) bool {
	//退出锁，在进行消息处理的时候不允许退出
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()
	//如果已经退出，则返回
	if c.Exiting() {
		return false
	}
	dirty := false
	for {
		//循环从延迟消息队列取出消息进行发送
		c.deferredMutex.Lock()
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()
		if item == nil {
			goto exit
		}
		dirty = true
		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		c.put(msg)
	}
exit:
	return dirty
}
//处理待处理消息队列
func (c *Channel) processInFlightQueue(t int64) bool {
	//退出锁，在进行消息处理的时候不允许退出
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()
	if c.Exiting() {
		return false
	}
	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()
		if msg == nil {
			goto exit
		}
		dirty = true
		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()
		}
		c.put(msg)
	}
exit:
	return dirty
}
