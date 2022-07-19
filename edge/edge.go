package edge

import (
	"github.com/stepneko/neko-dataflow/vertex"
)

type Edge interface {
	GetSrc() vertex.Id
	GetTarget() vertex.Id
}

type EdgeCore struct {
	src    vertex.Id
	target vertex.Id
}

func NewEdge(
	src vertex.Id,
	target vertex.Id,
) *EdgeCore {
	return &EdgeCore{
		src,
		target,
	}
}

func (e *EdgeCore) GetSrc() vertex.Id {
	return e.src
}

func (e *EdgeCore) GetTarget() vertex.Id {
	return e.target
}
