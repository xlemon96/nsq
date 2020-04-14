package nsqd

/*
	临时topic和channel采用空持久化队列，内存满了之后则消息丢弃
*/

type dummyBackendQueue struct {
	readChan chan []byte
}

func newDummyBackendQueue() BackendQueue {
	return &dummyBackendQueue{readChan: make(chan []byte)}
}

func (d *dummyBackendQueue) Put([]byte) error {
	return nil
}

func (d *dummyBackendQueue) ReadChan() <-chan []byte {
	return d.readChan
}

func (d *dummyBackendQueue) Close() error {
	return nil
}

func (d *dummyBackendQueue) Delete() error {
	return nil
}

func (d *dummyBackendQueue) Depth() int64 {
	return int64(0)
}

func (d *dummyBackendQueue) Empty() error {
	return nil
}
