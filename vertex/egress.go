package vertex

type EgressVertex struct {
	GenericVertex
}

func NewEgressVertex() *EgressVertex {
	vertex := EgressVertex{
		*NewGenericVertex(),
	}
	vertex.vertexType = VertexType_Egress
	return &vertex
}
