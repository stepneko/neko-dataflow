package operators

import (
	"errors"
	"sync"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/handles"
	"github.com/stepneko/neko-dataflow/request"
	"github.com/stepneko/neko-dataflow/scope"
	"github.com/stepneko/neko-dataflow/timestamp"
)

type OpCore struct {
	scope.Scope
	id     constants.VertexId
	typ    constants.VertexType
	currTs timestamp.Timestamp
	target constants.VertexId
}

func NewOpCore(
	vid constants.VertexId,
	typ constants.VertexType,
	scope scope.Scope,
) *OpCore {
	return &OpCore{
		Scope:  scope,
		id:     vid,
		typ:    typ,
		currTs: *timestamp.NewTimestamp(),
		target: constants.VertexId_Nil,
	}
}

// ===================== Impl Vertex interface ================== //
func (op *OpCore) Id() constants.VertexId {
	return op.id
}

func (op *OpCore) Type() constants.VertexType {
	return op.typ
}

func (op *OpCore) Start(wg sync.WaitGroup) error {
	return errors.New("raw opcore trying to run Start() with no operater specific Start()")
}

// ===================== Impl Operator interface ================== //

func (op *OpCore) AsScope() scope.Scope {
	return op.Scope
}

func (op *OpCore) SetTarget(vid constants.VertexId) {
	op.target = vid
}

func (op *OpCore) Inspect(f DataCallback) InspectOp {
	s := op.AsScope()

	taskCh := make(chan request.Request, constants.ChanCacapity)

	ackCh := make(chan request.Request, constants.ChanCacapity)

	handle := handles.NewLocalVertexHandle(taskCh, ackCh)

	vid := s.GenerateVID()

	v := &InspectOpCore{
		OpCore: NewOpCore(vid, constants.VertexType_Inspect, s),
		handle: handle,
		f:      f,
	}

	s.RegisterVertex(v, handle)
	s.RegisterEdge(op, v, handle)
	op.SetTarget(vid)

	return v
}

func (op *OpCore) Binary(other Operator, f1 DataCallback, f2 DataCallback) BinaryOp {
	s := op.AsScope()

	taskCh1 := make(chan request.Request, constants.ChanCacapity)
	taskCh2 := make(chan request.Request, constants.ChanCacapity)

	ackCh1 := make(chan request.Request, constants.ChanCacapity)
	ackCh2 := make(chan request.Request, constants.ChanCacapity)

	handle1 := handles.NewLocalVertexHandle(taskCh1, ackCh1)
	handle2 := handles.NewLocalVertexHandle(taskCh2, ackCh2)

	vid := s.GenerateVID()

	v := &BinaryOpCore{
		OpCore:  NewOpCore(vid, constants.VertexType_Bianry, s),
		handle1: handle1,
		handle2: handle2,

		f1: f1,
		f2: f2,
	}

	s.RegisterVertex(v, handle1)
	s.RegisterEdge(op, v, handle1)
	op.SetTarget(vid)
	s.RegisterEdge(other, v, handle2)
	other.SetTarget(vid)
	return v
}

// ================ Imple some core functions for common use case ============= //

func (op *OpCore) coreSendBy(
	e edge.Edge,
	msg request.Message,
	ts timestamp.Timestamp,
	handle handles.VertexHandle,
) error {
	// If no target specified for this operator, then no need to send the message
	if e.GetTarget() == constants.VertexId_Nil {
		return nil
	}

	if err := op.coreIncreOC(e, ts, handle); err != nil {
		return err
	}

	// Send the actual message to worker
	req := request.Request{
		Typ:  constants.RequestType_SendBy,
		Edge: e,
		Msg:  msg,
		Ts:   ts,
	}
	if err := op.GetWorkerHandle().Send(&req); err != nil {
		return err
	}
	return nil
}

func (op *OpCore) coreIncreOC(
	e edge.Edge,
	ts timestamp.Timestamp,
	handle handles.VertexHandle,
) error {
	// Handle IncreOC first
	incReq := request.Request{
		Typ:  constants.RequestType_IncreOC,
		Edge: e,
		Msg:  *request.NewMessage([]byte{}),
		Ts:   ts,
	}
	if err := op.GetWorkerHandle().Send(&incReq); err != nil {
		return err
	}
	<-handle.AckRecv()
	return nil
}

func (op *OpCore) coreDecreOC(
	e edge.Edge,
	ts timestamp.Timestamp,
	handle handles.VertexHandle,
) error {
	// Handle DecreOC first
	req := request.Request{
		Typ:  constants.RequestType_DecreOC,
		Edge: e,
		Msg:  *request.NewMessage([]byte{}),
		Ts:   ts,
	}
	if err := op.GetWorkerHandle().Send(&req); err != nil {
		return err
	}
	<-handle.AckRecv()
	return nil
}
