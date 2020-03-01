package nsqd

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"

	"nsq/internal/lg"
	"nsq/internal/quantile"
	"nsq/internal/util"
)

type Topic struct {
	//消息数量、消息总共的字节数量
	messageCount uint64
	messageBytes uint64
	//全局锁
	sync.RWMutex
	//topic名称
	name              string
	//topic所有的channel
	channelMap        map[string]*Channel
	//持久化队列
	backend           BackendQueue
	//内存chan
	memoryMsgChan     chan *Message
	//开始、结束、更新chan
	startChan         chan int
	exitChan          chan int
	channelUpdateChan chan int
	//包装函数
	waitGroup         util.WaitGroupWrapper
	//退出标志
	exitFlag          int32
	//生成uuid
	idFactory         *guidFactory
	//是否临时标志
	ephemeral      bool
	//删除此channel的清理函数(只有当订阅此topic的消费者数量为0，且此topic为临时topic才会调用)
	deleteCallback func(*Topic)
	//保证清理函数只执行一次
	deleter        sync.Once
	//暂停标志，暂定chan
	paused    int32
	pauseChan chan int
	//全局context，包含nsqd结构体
	ctx *context
}
//新建一个topic
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     nil,
		startChan:         make(chan int, 1),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		ctx:               ctx,
		paused:            0,
		pauseChan:         make(chan int),
		deleteCallback:    deleteCallback,
		idFactory:         NewGUIDFactory(ctx.nsqd.getOpts().ID),
	}
	if ctx.nsqd.getOpts().MemQueueSize > 0 {
		t.memoryMsgChan = make(chan *Message, ctx.nsqd.getOpts().MemQueueSize)
	}
	if strings.HasSuffix(topicName, "#ephemeral") {
		t.ephemeral = true
		t.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		t.backend = diskqueue.New(
			topicName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}
	t.waitGroup.Wrap(t.messagePump)
	t.ctx.nsqd.Notify(t)
	return t
}
//开启一个topic的loop
func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}
//topic是否退出，判断退出标致是否为1
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}
//获得相应名称的channel
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	//获取或者创建一个channel
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()
	//如果是新的channel，向更新channel写入数据
	if isNew {
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}
	return channel
}
//若channel不存在，则新建一个channel
//若channel存在，则返回
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.ctx, deleteCallback)
		t.channelMap[channelName] = channel
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}
//获取一个存在的channel
func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}
//删除一个存在的channel，赋值给channel的deleteFunc
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	t.Unlock()
	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)
	channel.Delete()
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}
	//每删除一个channel，判断一下是否需要清理该topic
	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}
	return nil
}
//放入一个消息到内存
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := t.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body)))
	return nil
}
//放入批量消息到内存
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	messageTotalBytes := 0
	for i, m := range msgs {
		err := t.put(m)
		if err != nil {
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
			return err
		}
		messageTotalBytes += len(m.Body)
	}
	atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}
//放入消息到channel，若内存已满，则放入到持久化队列当中
func (t *Topic) put(m *Message) error {
	select {
	case t.memoryMsgChan <- m:
	default:
		b := bufferPoolGet()
		err := writeMessageToBackend(b, m, t.backend)
		bufferPoolPut(b)
		t.ctx.nsqd.SetHealth(err)
		if err != nil {
			t.ctx.nsqd.logf(LOG_ERROR,
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				t.name, err)
			return err
		}
	}
	return nil
}
//返回消息数量
func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}
//消息泵
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan <-chan []byte
	for {
		select {
		case <-t.channelUpdateChan:
			continue
		case <-t.pauseChan:
			continue
		case <-t.exitChan:
			goto exit
		case <-t.startChan:
		}
		break
	}
	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()
	if len(chans) > 0 && !t.IsPaused() {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}
	//主消息循环
	for {
		select {
		case msg = <-memoryMsgChan:
		case buf = <-backendChan:
			msg, err = decodeMessage(buf)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
		case <-t.channelUpdateChan:
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.pauseChan:
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:
			goto exit
		}
		for i, channel := range chans {
			chanMsg := msg
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			if chanMsg.deferred != 0 {
				channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
				continue
			}
			err := channel.PutMessage(chanMsg)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR,
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}
exit:
	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing ... messagePump", t.name)
}
//删除topic
func (t *Topic) Delete() error {
	return t.exit(true)
}
//关闭topic
func (t *Topic) Close() error {
	return t.exit(false)
}
//主exit函数
func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}
	if deleted {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)
		t.ctx.nsqd.Notify(t)
	} else {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}
	close(t.exitChan)
	t.waitGroup.Wait()
	if deleted {
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()
		t.Empty()
		return t.backend.Delete()
	}
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			t.ctx.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}
	t.flush()
	return t.backend.Close()
}

func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}
finish:
	return t.backend.Empty()
}

func (t *Topic) flush() error {
	var msgBuf bytes.Buffer
	if len(t.memoryMsgChan) > 0 {
		t.ctx.nsqd.logf(LOG_INFO,
			"TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}
	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR,
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}
finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}
	select {
	case t.pauseChan <- 1:
	case <-t.exitChan:
	}
	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

func (t *Topic) GenerateID() MessageID {
retry:
	id, err := t.idFactory.NewGUID()
	if err != nil {
		time.Sleep(time.Millisecond)
		goto retry
	}
	return id.Hex()
}
