package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/timestamp"
)

type UnaryVertex struct {
	VertexCore
}

func NewUnaryVertex() *UnaryVertex {
	vertex := UnaryVertex{
		*NewVertexCore(),
	}
	vertex.typ = constants.VertexType_Unary
	return &vertex
}

func (v *UnaryVertex) OnRecv(f func(e Edge, m Message, ts timestamp.Timestamp) error) {
	v.internalOnRecv(f, constants.VertexInDir_Default)
}

func (v *UnaryVertex) OnNotify(f func(ts timestamp.Timestamp) error) {
	v.internalOnNotify(f, constants.VertexInDir_Default)
}
