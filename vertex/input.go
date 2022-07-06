package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/timestamp"
)

type InputVertex struct {
	VertexCore
}

func NewInputVertex() *InputVertex {
	vertex := InputVertex{
		*NewVertexCore(),
	}
	vertex.typ = constants.VertexType_Input
	return &vertex
}

func (i *InputVertex) Send(ts timestamp.Timestamp, m Message) {
	ch := i.GetInTaskChan()
	id := i.GetId()
	req := Request{
		Typ:  constants.RequestType_OnRecv,
		Edge: NewEdge(id, id),
		Ts:   ts,
		Msg:  m,
	}
	ch <- req
}

func (i *InputVertex) Notify(ts timestamp.Timestamp) {
	ch := i.GetInTaskChan()
	id := i.GetId()
	req := Request{
		Typ:  constants.RequestType_OnNotify,
		Edge: NewEdge(id, id),
		Ts:   ts,
		Msg:  Message{},
	}

	ch <- req
}
