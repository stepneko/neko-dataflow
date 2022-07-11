package vertex

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type Edge interface {
	GetSrc() constants.VertexId
	GetTarget() constants.VertexId
}

type EdgeCore struct {
	src    constants.VertexId
	target constants.VertexId
}

func NewEdge(
	src constants.VertexId,
	target constants.VertexId,
) *EdgeCore {
	return &EdgeCore{
		src,
		target,
	}
}

func (e *EdgeCore) GetSrc() constants.VertexId {
	return e.src
}

func (e *EdgeCore) GetTarget() constants.VertexId {
	return e.target
}
