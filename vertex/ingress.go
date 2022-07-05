package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type IngressVertex struct {
	GenericVertex
}

func NewIngressVertex() *IngressVertex {
	vertex := IngressVertex{
		*NewGenericVertex(),
	}
	vertex.typ = constants.VertexType_Ingress
	return &vertex
}
