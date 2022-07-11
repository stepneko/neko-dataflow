package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type EgressVertex struct {
	UnaryVertex
}

func NewEgressVertex() *EgressVertex {
	vertex := EgressVertex{
		*NewUnaryVertex(),
	}
	vertex.typ = constants.VertexType_Egress
	return &vertex
}
