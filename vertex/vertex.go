package vertex

import (
	"sync"
)

type Id int

const Id_Nil = 0

type Type int

const (
	Type_Input Type = iota
	Type_Ingress
	Type_IngressAdapter
	Type_Egress
	Type_EgressAdapter
	Type_Feedback
	Type_Inspect
	Type_Bianry
)

// Vertex is the interface that represents a vertex in the computing graph.
type Vertex interface {
	// Get Id of the vertex.
	Id() Id
	// Get Type of the vertex.
	Type() Type
	// Start starts the vertex.
	Start(wg sync.WaitGroup) error
}
