package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/timestamp"
)

type BinaryVertex struct {
	VertexCore // Vertex
}

func NewBinaryVertex() *BinaryVertex {
	vertex := BinaryVertex{
		*NewVertexCore(),
	}
	vertex.typ = constants.VertexType_Bianry
	return &vertex
}

func (v *BinaryVertex) OnRecvLeft(f func(e Edge, m Message, ts timestamp.Timestamp) error) {
	v.internalOnRecv(f, constants.VertexInDir_Left)
}

func (v *BinaryVertex) OnNotifyLeft(f func(ts timestamp.Timestamp) error) {
	v.internalOnNotify(f, constants.VertexInDir_Left)
}

func (v *BinaryVertex) OnRecvRight(f func(e Edge, m Message, ts timestamp.Timestamp) error) {
	v.internalOnRecv(f, constants.VertexInDir_Right)
}

func (v *BinaryVertex) OnNotifyRight(f func(ts timestamp.Timestamp) error) {
	v.internalOnNotify(f, constants.VertexInDir_Right)
}
