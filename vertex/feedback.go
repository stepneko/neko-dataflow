package vertex

type FeedbackVertex struct {
	GenericVertex
}

func NewFeedbackVertex() *FeedbackVertex {
	vertex := FeedbackVertex{
		*NewGenericVertex(),
	}
	vertex.vertexType = VertexType_Feedback
	return &vertex
}
