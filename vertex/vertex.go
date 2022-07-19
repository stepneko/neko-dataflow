package vertex

import (
	"sync"
)

type Id int

const Id_Nil = 0

type Type int

const (
	Type_Input          Type = 1
	Type_Ingress        Type = 2
	Type_IngressAdapter Type = 3
	Type_Egress         Type = 4
	Type_EgressAdapter  Type = 5
	Type_Feedback       Type = 6
	Type_Inspect        Type = 7
	Type_Bianry         Type = 8
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
