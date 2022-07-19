package scope

import (
	"github.com/stepneko/neko-dataflow/handles"
	"github.com/stepneko/neko-dataflow/vertex"
)

type Scope interface {
	// Name returns the name of the scope
	Name() string
	// GenerateVID generates a unique vertex id for new vertex
	GenerateVID() vertex.Id
	// GetWorkerHandle gets the handle of the worker
	GetWorkerHandle() handles.WorkerHandle
	// RegisterVertex registers a vertex with its handle to the scope
	RegisterVertex(v vertex.Vertex, handle handles.VertexHandle) error
	// RegisterEdge registers an edge with its target handle to the scope
	RegisterEdge(src vertex.Vertex, target vertex.Vertex, handle handles.VertexHandle) error
	// Done indicates that the scope is done with computation
	Done() <-chan struct{}
}
