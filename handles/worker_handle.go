package handles

import (
	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/request"
)

type WorkerHandle interface {
	Send(*request.Request) error
	Recv() chan request.Request
}

type SimpleWorkerHandle struct {
	ch chan request.Request
}

func NewSimpleWorkerHandle() *SimpleWorkerHandle {
	return &SimpleWorkerHandle{
		ch: make(chan request.Request, constants.ChanCapacity),
	}
}

func (t *SimpleWorkerHandle) Send(req *request.Request) error {
	t.ch <- *req
	return nil
}

func (t *SimpleWorkerHandle) Recv() chan request.Request {
	return t.ch
}
