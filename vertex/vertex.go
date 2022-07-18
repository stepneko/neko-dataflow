package vertex

import (
	"sync"

	"github.com/stepneko/neko-dataflow/constants"
)

// Vertex is the interface that represents a vertex in the computing graph.
type Vertex interface {
	// Get Id of the vertex.
	Id() constants.VertexId
	// Get Type of the vertex.
	Type() constants.VertexType
	// Start starts the vertex.
	Start(wg sync.WaitGroup) error
}
