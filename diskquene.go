package nsq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

/*
	diskqueme是nsq持久化消息的队列，当channel内存消息的channel满了之后，消息会往diskquene投递，diskquene会定期
	同步数据到具体的文件
*/

type LogLevel int

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

type Interface interface {
	Put([]byte) error
	ReadChan() <-chan []byte
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

type diskQueue struct {
	sync.RWMutex
	//读写游标、读写文件序号
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	//队列深度，即还有多少消息可以读
	depth        int64
	//diskquene名字，已经数据路径
	//通常name为topic:channel
	name            string
	dataPath        string
	//单个数据文件最大大小
	maxBytesPerFile int64
	//最大最小的消息大小
	minMsgSize      int32
	maxMsgSize      int32
	//触发同步的周期和io次数
	syncEvery       int64
	syncTimeout     time.Duration
	//循环退出标志
	exitFlag        int32
	//同步标志，true触发同步
	needSync        bool
	//下一次的读游标和文件序号
	//这两个参数主要用以保证是否已经有一个消息被上游读走
	//若nextReadPos=readPos，则消息已经读走，在movaFoward函数中触发
	nextReadPos     int64
	nextReadFileNum int64
	//读写文件句柄即相关buffer
	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer
	//读chan，用以上游读取diskquene的消息
	readChan chan []byte
	//深度chan
	depthChan         chan int64
	//写chan，写响应chan
	writeChan         chan []byte
	writeResponseChan chan error
	//置空diskquene chan，响应chan
	emptyChan         chan int
	emptyResponseChan chan error
	//退出chan
	exitChan          chan int
	//保证循环退出之后再结束diskquene队列
	exitSyncChan      chan int
	logf AppLogFunc
}
//实力化一个diskquene
func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		depthChan:         make(chan int64),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		logf:              logf,
	}
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}
	go d.ioLoop()
	return &d
}
//返回队列深度，即还有多少消息没有读取
//之所以采用chan形式，可以保证在读取深度这一行为时不会同时有读写行为在发生
//否则需要通过lock形式来保证深度数据的正确性
func (d *diskQueue) Depth() int64 {
	return <-d.depthChan
}
//返回一个读chan，获取从硬盘读取的消息
func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readChan
}
//放入一个消息到diskquene，阻塞函数，直到数据写入持久化文件
func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()
	if d.exitFlag == 1 {
		return errors.New("exiting")
	}
	d.writeChan <- data
	return <-d.writeResponseChan
}
//关闭循环，此函数会调用sync函数，将数据强行刷盘，不会导致数据丢失
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}
//退出循环，只是决定日志打印内容
func (d *diskQueue) Delete() error {
	return d.exit(true)
}
//退出diskquene循环
func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()
	d.exitFlag = 1
	if deleted {
		d.logf(INFO, "DISKQUEUE(%s): deleting", d.name)
	} else {
		d.logf(INFO, "DISKQUEUE(%s): closing", d.name)
	}
	//关闭diskquene循环
	close(d.exitChan)
	//确保循环已经退出
	<-d.exitSyncChan
	//关闭读写文件句柄
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}
	return nil
}
//将此diskquene数据置空
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()
	//若循环已经退出，则返回error
	if d.exitFlag == 1 {
		return errors.New("exiting")
	}
	d.logf(INFO, "DISKQUEUE(%s): emptying", d.name)
	d.emptyChan <- 1
	return <-d.emptyResponseChan
}
//删除所有的文件
//1、跳转到下一个可读可写文件，相当于删除有数据的文件
//2、删除元数据文件
func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()
	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}
	return err
}
//跳到下一个可读可写文件
//若此时在读*000001.dat
//在写*000003.dat
//则此函数直接将读写文件均到*000004.dat
func (d *diskQueue) skipToNextRWFile() error {
	var err error
	//若此时读写文件均不为空，则关闭文件句柄，置空文件指针
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}
	//将已写未读的文件均删除
	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}
	//将相关游标属性重置
	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	d.depth = 0
	return err
}
//此函数为从数据文件当中读取一个消息
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32
	//readFile为nil有以下情况
	//1、diskquene初始化的时候
	//2、读完一个文件的时候
	//3、读取文件异常的时候
	//4、跳转到下一个可读可写的时候
	//此时需要重新打开文件
	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}
		d.logf(INFO, "DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)
		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}
		d.reader = bufio.NewReader(d.readFile)
	}
	//读取消息长度
	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}
	//若消息长度不符合要求，则证明文件损坏
	//-----------------
	//-----------------
	//此处有疑问，若此处仅仅关闭文件句柄并置为空，则下次读取数据依旧有问题
	//为何不将读文件序号加+1，将此文件作废
	//-----------------
	//-----------------
	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}
	//读取消息体
	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}
	//更新下次读取位置和下次读取文件序号
	totalBytes := int64(4 + msgSize)
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum
	//若下次读位置超过但文件最大大小
	//读文件序号加1，读游标置为0
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}
		d.nextReadFileNum++
		d.nextReadPos = 0
	}
	return readBuf, nil
}
//此函数为从数据文件当中写入一个消息
func (d *diskQueue) writeOne(data []byte) error {
	var err error
	//writeFile为nil有以下情况
	//1、diskquene初始化的时候
	//2、达到同步条件，同步完成之后
	//3、写入消息的时候发生异常
	//4、跳转到下一个可读可写的时候
	//此时需要重新打开文件
	if d.writeFile == nil {
		//通过此时写文件序号返回待写文件文件名称
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		d.logf(INFO, "DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)
		//若写游标大于0，则需要将写游标跳转到对应的位置
		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}
	dataLen := int32(len(data))
	//判断消息长度，异常则直接返回
	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}
	//重置写buffer
	d.writeBuf.Reset()
	//写入消息长度
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}
	//写入消息体
	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}
	//将写buffer的内容写入文件
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}
	//将写位置跳转相应的长度
	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	d.depth += 1
	//如果当前写的游标大于了文件最大的大小
	//则需要新开启一个文件
	//因此，单个文件的大小可能会超过最大的大小，但最多不会超过一个消息的大小
	if d.writePos >= d.maxBytesPerFile {
		//若内容已经溢出，则写文件序号加1，写游标置为0
		//同时将文件内容强行刷到硬盘
		d.writeFileNum++
		d.writePos = 0
		err = d.sync()
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}
		//若刷数据成功，则文件句柄不为空，需要关闭溢出数据文件，并置文件句柄为空
		//若刷数据失败，则sync函数会关闭溢出数据文件，并置文件句柄为空
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}
	return err
}
//同步内存中的消息到数据文件
//主要就是：
// 	  1、将此时打开的写文件强行刷新到硬盘，并将写文件置为nil
//	  2、跟新元数据到元数据文件，同步标志置为false
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}
	err := d.persistMetaData()
	if err != nil {
		return err
	}
	d.needSync = false
	return nil
}
//解析元数据到内存
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error
	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	var depth int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	d.depth = depth
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos
	return nil
}
//持久化元数据到文件
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error
	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		d.depth,
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()
	//确保最新元数据写入文件再更改文件名，覆盖之前的文件名
	return os.Rename(tmpFileName, fileName)
}
//获取持久化元数据文件名
//形如：
//    /var/log/data/movie.diskquene.meta.dat
//    |  datapath  |name|
func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}
//获取持久化消息文件名
//形如：
//    /var/log/data/movie.diskquene.000001.dat
//    |  datapath  |name|          |filenum|
func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}
//检查读写进度并且校验数据是否发生异常
func (d *diskQueue) checkTailCorruption(depth int64) {
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}
	//进入后面的条件为readFileNum=writeFileNum并且readPos=writePos
	if depth != 0 {
		if depth < 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		d.depth = 0
		d.needSync = true
	}
	//若出现两者中的一个，则文件出现异常，跳转打下一个可读可写文件
	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			d.logf(ERROR,
				"DISKQUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}
		if d.readPos > d.writePos {
			d.logf(ERROR,
				"DISKQUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}
		d.skipToNextRWFile()
		d.needSync = true
	}
}
//将读相关游标往前移动
func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	d.depth -= 1
	//若旧文件序号不等于新文件序号，则证明就文件依旧读完，需要删除
	if oldReadFileNum != d.nextReadFileNum {
		//当读取新文件时强行刷新
		d.needSync = true
		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
		}
	}
	d.checkTailCorruption(d.depth)
}
//处理读错误
func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}
	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"
	d.logf(WARN,
		"DISKQUEUE(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)
	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(ERROR,
			"DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}
	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0
	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}
//diskquene的ioloop
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte
	//定期同步定时器
	syncTicker := time.NewTicker(d.syncTimeout)
	for {
		//达到了io同步次数，则置sync为true进行数据刷盘
		if count == d.syncEvery {
			d.needSync = true
		}
		//若需要sync，则刷盘，并置count为0
		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			count = 0
		}
		//满足readFileNum小于writeFileNum或者readPos小于writePos，则认为有数据可读，可以进入读流程
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			//若nextReadPos==readPos，则证明上游channel或者topic已经读取走一个消息，
			//则此时可以读取一个消息，在moveForward函数中触发nextReadPos==readPos
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(ERROR, "DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			//只有当dataRead重新赋值，才能将readChan暴露出去，否则，将会把旧数据重新发送到上游
			//因此若没有发生dataRead重新赋值，将readChan置为空
			r = d.readChan
		} else {
			r = nil
		}
		select {
		case r <- dataRead:
			count++
			d.moveForward()
		case d.depthChan <- d.depth:
		case <-d.emptyChan:
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeChan:
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		case <-syncTicker.C:
			if count == 0 {
				//若此时无io行为，则无需sync，提升性能
				continue
			}
			d.needSync = true
		case <-d.exitChan:
			goto exit
		}
	}
exit:
	d.logf(INFO, "DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}


