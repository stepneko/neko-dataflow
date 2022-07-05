package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type EgressVertex struct {
	GenericVertex
}

func NewEgressVertex() *EgressVertex {
	vertex := EgressVertex{
		*NewGenericVertex(),
	}
	vertex.typ = constants.VertexType_Egress
	return &vertex
}
