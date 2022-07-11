package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
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
