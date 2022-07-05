package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/timestamp"
)

type InputVertex struct {
	GenericVertex
}

func NewInputVertex() *InputVertex {
	vertex := InputVertex{
		*NewGenericVertex(),
	}
	vertex.typ = constants.VertexType_Input
	return &vertex
}

func (i *InputVertex) Send(ts timestamp.Timestamp, m Message) {
	ch := i.GetInTaskChan()
	req := Request{
		Typ:  constants.RequestType_OnRecv,
		Edge: NewEdge(i, i),
		Ts:   ts,
		Msg:  m,
	}
	ch <- req
}

func (i *InputVertex) Notify(ts timestamp.Timestamp) {
	ch := i.GetInTaskChan()
	req := Request{
		Typ:  constants.RequestType_OnNotify,
		Edge: NewEdge(i, i),
		Ts:   ts,
		Msg:  Message{},
	}

	ch <- req
}
