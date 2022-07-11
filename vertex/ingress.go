package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type IngressVertex struct {
	UnaryVertex
}

func NewIngressVertex() *IngressVertex {
	vertex := IngressVertex{
		*NewUnaryVertex(),
	}
	vertex.typ = constants.VertexType_Ingress
	return &vertex
}
