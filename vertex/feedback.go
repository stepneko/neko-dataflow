package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type FeedbackVertex struct {
	VertexCore
}

func NewFeedbackVertex() *FeedbackVertex {
	vertex := FeedbackVertex{
		*NewVertexCore(),
	}
	vertex.typ = constants.VertexType_Feedback
	return &vertex
}
