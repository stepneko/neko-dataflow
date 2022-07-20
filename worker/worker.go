package worker

import (
	"github.com/stepneko/neko-dataflow/scope"
)

type Id int

type DataflowFunc = func(s scope.Scope) error

// Worker is a worker unit containing a dataflow graph,
// taking inputs and making computations.
type Worker interface {
	Dataflow(fn DataflowFunc) error
	ToScope() scope.Scope
	Run() error
}
