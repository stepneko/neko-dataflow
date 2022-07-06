package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type GenericVertex struct {
	VertexCore
}

func NewGenericVertex() *GenericVertex {
	vertex := GenericVertex{
		*NewVertexCore(),
	}
	vertex.typ = constants.VertexType_Generic
	return &vertex
}
