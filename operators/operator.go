package operators

import (
	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/handles"
	"github.com/stepneko/neko-dataflow/request"
	"github.com/stepneko/neko-dataflow/scope"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/vertex"
)

type DataCallback func(e edge.Edge, msg request.Message, ts timestamp.Timestamp) (request.Message, error)

type Operator interface {
	vertex.Vertex
	SetTarget(vid constants.VertexId)
	Binary(other Operator, f1 DataCallback, f2 DataCallback) BinaryOp
	Inspect(f DataCallback) InspectOp
}

type GenericUnaryOp interface {
	OnRecv(e edge.Edge, msg request.Message, ts timestamp.Timestamp) error
	OnNotify(ts timestamp.Timestamp) error
	SendBy(e edge.Edge, msg request.Message, ts timestamp.Timestamp) error
	NotifyAt(ts timestamp.Timestamp) error
}

type GenericBinaryOp interface {
	OnRecv1(e edge.Edge, msg request.Message, ts timestamp.Timestamp) error
	OnRecv2(e edge.Edge, msg request.Message, ts timestamp.Timestamp) error
	OnNotify1(ts timestamp.Timestamp) error
	OnNotify2(ts timestamp.Timestamp) error
	SendBy(e edge.Edge, msg request.Message, ts timestamp.Timestamp) error
	NotifyAt(ts timestamp.Timestamp) error
}

// NewInput creates input operator from scope
func NewInput(s scope.Scope, inputCh chan request.InputRaw) Operator {
	taskCh := make(chan request.Request, constants.ChanCacapity)
	ackCh := make(chan request.Request, constants.ChanCacapity)

	handle := handles.NewLocalVertexHandle(taskCh, ackCh)

	vid := s.GenerateVID()

	v := &InputOpCore{
		OpCore:  NewOpCore(vid, constants.VertexType_Input, s),
		handle:  handle,
		inputCh: inputCh,
	}

	s.RegisterVertex(v, handle)
	return v
}
