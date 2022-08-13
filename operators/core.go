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
	"github.com/stepneko/neko-dataflow/vertex"
)

type OpCore struct {
	scope.Scope
	id     vertex.Id
	typ    vertex.Type
	currTs timestamp.Timestamp
	target vertex.Id
}

func NewOpCore(
	vid vertex.Id,
	typ vertex.Type,
	scope scope.Scope,
) *OpCore {
	return &OpCore{
		Scope:  scope,
		id:     vid,
		typ:    typ,
		currTs: *timestamp.NewTimestamp(),
		target: vertex.Id_Nil,
	}
}

// ===================== Impl Vertex interface ================== //
func (op *OpCore) Id() vertex.Id {
	return op.id
}

func (op *OpCore) Type() vertex.Type {
	return op.typ
}

func (op *OpCore) Start(wg *sync.WaitGroup) error {
	return errors.New("raw opcore trying to run Start() with no operater specific Start()")
}

// ===================== Impl Operator interface ================== //

func (op *OpCore) AsScope() scope.Scope {
	return op.Scope
}

func (op *OpCore) SetTarget(vid vertex.Id) {
	op.target = vid
}

func (op *OpCore) Inspect(f DataCallback) InspectOp {
	s := op.AsScope()

	taskCh := make(chan request.Request, constants.ChanCapacity)

	ackCh := make(chan request.Request, constants.ChanCapacity)

	handle := handles.NewLocalVertexHandle(taskCh, ackCh)

	vid := s.GenerateVID()

	v := &InspectOpCore{
		OpCore: NewOpCore(vid, vertex.Type_Inspect, s),
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

	taskCh1 := make(chan request.Request, constants.ChanCapacity)
	taskCh2 := make(chan request.Request, constants.ChanCapacity)

	ackCh1 := make(chan request.Request, constants.ChanCapacity)
	ackCh2 := make(chan request.Request, constants.ChanCapacity)

	handle1 := handles.NewLocalVertexHandle(taskCh1, ackCh1)
	handle2 := handles.NewLocalVertexHandle(taskCh2, ackCh2)

	vid := s.GenerateVID()

	v := &BinaryOpCore{
		OpCore:  NewOpCore(vid, vertex.Type_Bianry, s),
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

func (op *OpCore) Filter(f FilterCallback) FilterOp {
	s := op.AsScope()

	taskCh := make(chan request.Request, constants.ChanCapacity)

	ackCh := make(chan request.Request, constants.ChanCapacity)

	handle := handles.NewLocalVertexHandle(taskCh, ackCh)

	vid := s.GenerateVID()

	v := &FilterOpCore{
		OpCore: NewOpCore(vid, vertex.Type_Inspect, s),
		handle: handle,
		f:      f,
	}

	s.RegisterVertex(v, handle)
	s.RegisterEdge(op, v, handle)
	op.SetTarget(vid)

	return v
}

// Loop creates a loop structure in diagram:
// op -> Ingress -[OnRecv1]-> IngressAdapter -> loop struct[func(ups)] -> EgressAdapter -[target1]-> Egress -> Onward...
//                                  ^                                            |
//                        [OnRecv2] |                                            |[target2]
//                                  +------------------Feedback------------------+
func (op *OpCore) Loop(dataF func(ups Operator) Operator, filterF FilterCallback) EgressOp {
	s := op.AsScope()

	// Create ingress operator
	ingressTaskCh1 := make(chan request.Request, constants.ChanCapacity)
	ingressAckCh1 := make(chan request.Request, constants.ChanCapacity)

	ingressHandle := handles.NewLocalVertexHandle(ingressTaskCh1, ingressAckCh1)

	ingressVid := s.GenerateVID()

	ingressOp := &IngressOpCore{
		OpCore: NewOpCore(ingressVid, vertex.Type_Ingress, s),
		handle: ingressHandle,
	}

	s.RegisterVertex(ingressOp, ingressHandle)
	s.RegisterEdge(op, ingressOp, ingressHandle)
	op.SetTarget(ingressVid)

	// Create ingress adapter operator
	ingressAdpTaskCh1 := make(chan request.Request, constants.ChanCapacity)
	ingressAdpTaskCh2 := make(chan request.Request, constants.ChanCapacity)

	ingressAdpAckCh1 := make(chan request.Request, constants.ChanCapacity)
	ingressAdpAckCh2 := make(chan request.Request, constants.ChanCapacity)

	ingressAdpHandle1 := handles.NewLocalVertexHandle(ingressAdpTaskCh1, ingressAdpAckCh1)
	ingressAdpHandle2 := handles.NewLocalVertexHandle(ingressAdpTaskCh2, ingressAdpAckCh2)

	ingressAdpVid := s.GenerateVID()

	ingressAdpOp := &IngressAdapterOpCore{
		OpCore:  NewOpCore(ingressAdpVid, vertex.Type_IngressAdapter, s),
		handle1: ingressAdpHandle1,
		handle2: ingressAdpHandle2,
	}

	s.RegisterVertex(ingressAdpOp, ingressAdpHandle1)
	s.RegisterEdge(ingressOp, ingressAdpOp, ingressAdpHandle1)
	ingressOp.SetTarget(ingressAdpVid)

	// Make loop struct
	tailOp := dataF(ingressAdpOp)

	// Create egress adapter operator
	egressAdpTaskCh := make(chan request.Request, constants.ChanCapacity)
	egressAdpAckCh := make(chan request.Request, constants.ChanCapacity)

	egressAdpHandle := handles.NewLocalVertexHandle(egressAdpTaskCh, egressAdpAckCh)

	egressAdpVid := s.GenerateVID()

	egressAdpOp := &EgressAdapterOpCore{
		OpCore:  NewOpCore(egressAdpVid, vertex.Type_EgressAdapter, s),
		handle:  egressAdpHandle,
		target2: vertex.Id_Nil,
		f:       filterF,
	}

	s.RegisterVertex(egressAdpOp, egressAdpHandle)
	s.RegisterEdge(tailOp, egressAdpOp, egressAdpHandle)
	tailOp.SetTarget(egressAdpVid)

	// Create feedback operator
	feedbackTaskCh := make(chan request.Request, constants.ChanCapacity)
	feedbackAckCh := make(chan request.Request, constants.ChanCapacity)

	feedbackHandle := handles.NewLocalVertexHandle(feedbackTaskCh, feedbackAckCh)

	feedbackVid := s.GenerateVID()

	feedbackOp := &FeedbackOpCore{
		OpCore: NewOpCore(feedbackVid, vertex.Type_Feedback, s),
		handle: feedbackHandle,
	}

	s.RegisterVertex(feedbackOp, feedbackHandle)
	s.RegisterEdge(egressAdpOp, feedbackOp, feedbackHandle)
	egressAdpOp.SetTarget2(feedbackVid)

	s.RegisterEdge(feedbackOp, ingressAdpOp, ingressAdpHandle2)
	feedbackOp.SetTarget(ingressAdpVid)

	// Create egress adapter
	egressTaskCh := make(chan request.Request, constants.ChanCapacity)
	egressAckCh := make(chan request.Request, constants.ChanCapacity)

	egressHandle := handles.NewLocalVertexHandle(egressTaskCh, egressAckCh)

	egressVid := s.GenerateVID()

	egressOp := &EgressOpCore{
		OpCore: NewOpCore(egressVid, vertex.Type_Egress, s),
		handle: egressHandle,
	}

	s.RegisterVertex(egressOp, egressHandle)
	s.RegisterEdge(egressAdpOp, egressOp, egressHandle)
	egressAdpOp.SetTarget(egressVid)
	return egressOp
}

// ================ Imple some core functions for common use case ============= //

func (op *OpCore) coreSendBy(
	e edge.Edge,
	msg *request.Message,
	ts timestamp.Timestamp,
	handle handles.VertexHandle,
) error {
	// If no target specified for this operator, then no need to send the message
	if e.GetTarget() == vertex.Id_Nil {
		return nil
	}

	if err := op.coreIncreOC(e, ts, handle); err != nil {
		return err
	}

	// Send the actual message to worker
	req := request.Request{
		Type: request.Type_SendBy,
		Edge: e,
		Msg:  *msg,
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
		Type: request.Type_IncreOC,
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
		Type: request.Type_DecreOC,
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

func (op *OpCore) tsCheckAndUpdate(ts *timestamp.Timestamp) error {
	if !(timestamp.LE(&op.currTs, ts)) {
		return errors.New("cannot accept an earlier timestamp from inspect operator")
	}
	op.currTs = *ts
	return nil
}
