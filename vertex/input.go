package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/timestamp"
)

type InputVertex struct {
	UnaryVertex
}

func NewInputVertex() *InputVertex {
	vertex := InputVertex{
		*NewUnaryVertex(),
	}
	vertex.typ = constants.VertexType_Input
	return &vertex
}

func (v *InputVertex) Send(ts timestamp.Timestamp, m Message) {
	ch := v.GetInTaskChans()[constants.VertexInDir_Default]
	id := v.GetId()
	req := Request{
		Typ:  constants.RequestType_OnRecv,
		Edge: NewEdge(id, id),
		Ts:   ts,
		Msg:  m,
	}
	ch <- req
}

func (v *InputVertex) Notify(ts timestamp.Timestamp) {
	ch := v.GetInTaskChans()[constants.VertexInDir_Default]
	id := v.GetId()
	req := Request{
		Typ:  constants.RequestType_OnNotify,
		Edge: NewEdge(id, id),
		Ts:   ts,
		Msg:  Message{},
	}

	ch <- req
}
