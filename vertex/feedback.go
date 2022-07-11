package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type FeedbackVertex struct {
	UnaryVertex
}

func NewFeedbackVertex() *FeedbackVertex {
	vertex := FeedbackVertex{
		*NewUnaryVertex(),
	}
	vertex.typ = constants.VertexType_Feedback
	return &vertex
}
