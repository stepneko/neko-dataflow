package operators

import (
	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/iterator"
	"github.com/stepneko/neko-dataflow/request"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/vertex"
)

type DataCallback func(
	e edge.Edge,
	msg *request.Message,
	ts timestamp.Timestamp,
) (iterator.Iterator[*request.Message], error)

type FilterCallback func(
	e edge.Edge,
	msg *request.Message,
	ts timestamp.Timestamp,
) (bool, error)

type Operator interface {
	vertex.Vertex
	SetTarget(vid constants.VertexId)
	Binary(other Operator, f1 DataCallback, f2 DataCallback) BinaryOp
	Inspect(f DataCallback) InspectOp
	Filter(f FilterCallback) FilterOp
	Loop(dataF func(ups Operator) Operator, filterF FilterCallback) EgressOp
}

type SingleInput interface {
	OnRecv(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error
	OnNotify(ts timestamp.Timestamp) error
	SendBy(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error
	NotifyAt(ts timestamp.Timestamp) error
}

type DoubleInput interface {
	OnRecv1(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error
	OnRecv2(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error
	OnNotify1(ts timestamp.Timestamp) error
	OnNotify2(ts timestamp.Timestamp) error
	SendBy(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error
	NotifyAt(ts timestamp.Timestamp) error
}

type DoubleOutput interface {
	SetTarget2(vid constants.VertexId)
}
