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

func (op *OpCore) Filter(f FilterCallback) FilterOp {
	s := op.AsScope()

	taskCh := make(chan request.Request, constants.ChanCacapity)

	ackCh := make(chan request.Request, constants.ChanCacapity)

	handle := handles.NewLocalVertexHandle(taskCh, ackCh)

	vid := s.GenerateVID()

	v := &FilterOpCore{
		OpCore: NewOpCore(vid, constants.VertexType_Inspect, s),
		handle: handle,
		f:      f,
	}

	s.RegisterVertex(v, handle)
	s.RegisterEdge(op, v, handle)
	op.SetTarget(vid)

	return v
}

// Loop creates a loop structure in diagram:
// op --[OnRecv1]--> Ingress(as ups) --> loop struct[func(ups)] --> Egress --[target1]--> Onward...
//                       ^                                            |
// 			   [OnRecv2] |                                            |[target2]
//                       +------------------Feedback------------------+
func (op *OpCore) Loop(dataF func(ups Operator) Operator, filterF FilterCallback) EgressOp {
	s := op.AsScope()

	// Create ingress operator
	ingressTaskCh1 := make(chan request.Request, constants.ChanCacapity)
	ingressTaskCh2 := make(chan request.Request, constants.ChanCacapity)

	ingressAckCh1 := make(chan request.Request, constants.ChanCacapity)
	ingressAckCh2 := make(chan request.Request, constants.ChanCacapity)

	ingressHandle1 := handles.NewLocalVertexHandle(ingressTaskCh1, ingressAckCh1)
	ingressHandle2 := handles.NewLocalVertexHandle(ingressTaskCh2, ingressAckCh2)

	ingressVid := s.GenerateVID()

	ingressOp := &IngressOpCore{
		OpCore:  NewOpCore(ingressVid, constants.VertexType_Ingress, s),
		handle1: ingressHandle1,
		handle2: ingressHandle2,
	}

	s.RegisterVertex(ingressOp, ingressHandle1)
	s.RegisterEdge(op, ingressOp, ingressHandle1)
	op.SetTarget(ingressVid)

	// Make loop struct
	tailOp := dataF(ingressOp)

	// Create egress operator
	egressTaskCh := make(chan request.Request, constants.ChanCacapity)
	egressAckCh := make(chan request.Request, constants.ChanCacapity)

	egressHandle := handles.NewLocalVertexHandle(egressTaskCh, egressAckCh)

	egressVid := s.GenerateVID()

	egressOp := &EgressOpCore{
		OpCore:  NewOpCore(egressVid, constants.VertexType_Egress, s),
		handle:  egressHandle,
		target2: constants.VertexId_Nil,
		f:       filterF,
	}

	s.RegisterVertex(egressOp, egressHandle)
	s.RegisterEdge(tailOp, egressOp, egressHandle)
	tailOp.SetTarget(egressVid)

	// Create feedback operator
	feedbackTaskCh := make(chan request.Request, constants.ChanCacapity)
	feedbackAckCh := make(chan request.Request, constants.ChanCacapity)

	feedbackHandle := handles.NewLocalVertexHandle(feedbackTaskCh, feedbackAckCh)

	feedbackVid := s.GenerateVID()

	feedbackOp := &FeedbackOpCore{
		OpCore: NewOpCore(feedbackVid, constants.VertexType_Feedback, s),
		handle: feedbackHandle,
	}

	s.RegisterVertex(feedbackOp, feedbackHandle)
	s.RegisterEdge(egressOp, feedbackOp, feedbackHandle)
	// Use SetTarget2() because feedbackOp works as target2 of egress operator
	// so that SetTarget() could be used to connect to downstream operators
	// in the dataflow graph onwards
	egressOp.SetTarget2(feedbackVid)

	s.RegisterEdge(feedbackOp, ingressOp, ingressHandle2)
	feedbackOp.SetTarget(ingressVid)
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

func (op *OpCore) tsCheckAndUpdate(ts *timestamp.Timestamp) error {
	if !(timestamp.LE(&op.currTs, ts)) {
		return errors.New("cannot accept an earlier timestamp from inspect operator")
	}
	op.currTs = *ts
	return nil
}
