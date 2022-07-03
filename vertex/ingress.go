package vertex

type IngressVertex struct {
	GenericVertex
}

func NewIngressVertex() *IngressVertex {
	vertex := IngressVertex{
		*NewGenericVertex(),
	}
	vertex.vertexType = VertexType_Ingress
	return &vertex
}
