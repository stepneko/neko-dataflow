package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type FeedbackVertex struct {
	GenericVertex
}

func NewFeedbackVertex() *FeedbackVertex {
	vertex := FeedbackVertex{
		*NewGenericVertex(),
	}
	vertex.typ = constants.VertexType_Feedback
	return &vertex
}
