package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type IngressVertex struct {
	VertexCore
}

func NewIngressVertex() *IngressVertex {
	vertex := IngressVertex{
		*NewVertexCore(),
	}
	vertex.typ = constants.VertexType_Ingress
	return &vertex
}
