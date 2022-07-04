package vertex

import (
	"github.com/stepneko/neko-dataflow/timestamp"
)

type InputVertex struct {
	GenericVertex
}

func NewInputVertex() *InputVertex {
	vertex := InputVertex{
		*NewGenericVertex(),
	}
	vertex.vertexType = VertexType_Input
	return &vertex
}

func (i *InputVertex) Send(ts timestamp.Timestamp, m Message) {
	ch := i.GetInTaskChan()
	req := NewRequest(
		CallbackType_OnRecv,
		NewEdge(i, i),
		ts,
		m,
	)
	ch <- *req
}

func (i *InputVertex) Notify(ts timestamp.Timestamp) {
	ch := i.GetInTaskChan()
	req := NewRequest(
		CallbackType_OnNotify,
		NewEdge(i, i),
		ts,
		Message{},
	)
	ch <- *req
}
