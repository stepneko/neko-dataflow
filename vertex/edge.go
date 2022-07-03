package vertex

type Edge interface {
	GetSrc() Vertex
	GetTarget() Vertex
}

type EdgeCore struct {
	src    Vertex
	target Vertex
}

func NewEdge(src Vertex, target Vertex) *EdgeCore {
	return &EdgeCore{
		src,
		target,
	}
}

func (e *EdgeCore) GetSrc() Vertex {
	return e.src
}

func (e *EdgeCore) GetTarget() Vertex {
	return e.target
}
