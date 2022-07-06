package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type EgressVertex struct {
	VertexCore
}

func NewEgressVertex() *EgressVertex {
	vertex := EgressVertex{
		*NewVertexCore(),
	}
	vertex.typ = constants.VertexType_Egress
	return &vertex
}
