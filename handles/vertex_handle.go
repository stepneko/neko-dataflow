package handles

import (
	"github.com/stepneko/neko-dataflow/request"
)

// VertexHandle is the handle that is held by worker to communicate with Vertex.
// If the vertex is within the scope of a worker, then the handler contains a chan.
// Otherwise it contains network sockets to communicate with workers in other processes.
type VertexHandle interface {
	Send(req *request.Request)
	Ack(req *request.Request)
	MsgRecv() chan request.Request
	AckRecv() chan request.Request
}

type LocalVertexHandle struct {
	taskCh chan request.Request
	ackCh  chan request.Request
}

func NewLocalVertexHandle(
	taskCh chan request.Request,
	ackCh chan request.Request,
) *LocalVertexHandle {
	return &LocalVertexHandle{
		taskCh: taskCh,
		ackCh:  ackCh,
	}
}

func (h *LocalVertexHandle) Send(req *request.Request) {
	h.taskCh <- *req
}

func (h *LocalVertexHandle) Ack(req *request.Request) {
	h.ackCh <- *req
}

func (h *LocalVertexHandle) MsgRecv() chan request.Request {
	return h.taskCh
}

func (h *LocalVertexHandle) AckRecv() chan request.Request {
	return h.ackCh
}
