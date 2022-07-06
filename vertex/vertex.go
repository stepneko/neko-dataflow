package vertex

import (
	"context"
	"errors"
	"sync"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/timestamp"
)

// Vertex is the interface that represents a vertex in the computing graph.
type Vertex interface {
	// Set context of the vertex.
	SetContext(ctx context.Context)
	// Set Id of the vertex.
	SetId(id constants.VertexId)
	// Get Id of the vertex.
	GetId() constants.VertexId
	// Get Type of the vertex.
	GetType() constants.VertexType
	// SetExtChan sets the chan handle sending requests to scheduler.
	SetExtChan(ch chan Request)
	// GetExtChan gets the chan handle sending requests to scheduler.
	GetExtChan() chan Request
	// SetInTaskChan sets the chan handle receiving task requests from scheduler.
	SetInTaskChan(ch chan Request)
	// GetInTaskChan gets the chan handle receiving task requests from scheduler.
	GetInTaskChan() chan Request
	// SetInAckChan sets the chan handle receiving acks from scheduler.
	SetInAckChan(ch chan Request)
	// GetInAckChan gets the chan handle receiving acks from scheduler.
	GetInAckChan() chan Request
	// Handle triggers the processing of request.
	Handle(req *Request) error
	// SendBy is a wrapper that triggers the RequestType_SendBy function
	SendBy(e Edge, m Message, ts timestamp.Timestamp) error
	// NotifyAt is a wrapper that triggers the RequestType_NotifyAt function
	NotifyAt(ts timestamp.Timestamp) error
	// OnRecv registers RequestType_OnRecv function
	OnRecv(f func(e Edge, m Message, ts timestamp.Timestamp) error)
	// OnNotify registers RequestType_OnNotify function
	OnNotify(f func(ts timestamp.Timestamp) error)
	// Start starts a runtime for the vertex to handle dataflow.
	Start(wg sync.WaitGroup) error
}

type VertexCore struct {
	ctx      context.Context
	id       constants.VertexId
	typ      constants.VertexType
	extCh    chan Request
	inTaskCh chan Request
	inAckCh  chan Request

	FuncOnRecv   func(e Edge, m Message, ts timestamp.Timestamp) error
	FuncOnNotify func(ts timestamp.Timestamp) error
}

func NewVertexCore() *VertexCore {
	return &VertexCore{
		ctx:      nil,
		id:       0,
		typ:      constants.VertexType_Generic,
		extCh:    nil,
		inTaskCh: nil,
		inAckCh:  nil,

		FuncOnRecv:   nil,
		FuncOnNotify: nil,
	}
}

func (v *VertexCore) Start(wg sync.WaitGroup) error {
	defer wg.Done()
	for {
		select {
		case req := <-v.GetInTaskChan():
			v.Handle(&req)
		case <-v.ctx.Done():
			return nil
		}
	}
}

func (v *VertexCore) SetContext(ctx context.Context) {
	v.ctx = ctx
}

func (v *VertexCore) SetId(id constants.VertexId) {
	v.id = id
}

func (v *VertexCore) GetId() constants.VertexId {
	return v.id
}

func (v *VertexCore) GetType() constants.VertexType {
	return v.typ
}

func (v *VertexCore) SetExtChan(ch chan Request) {
	v.extCh = ch
}

func (v *VertexCore) GetExtChan() chan Request {
	return v.extCh
}

func (v *VertexCore) SetInTaskChan(ch chan Request) {
	v.inTaskCh = ch
}

func (v *VertexCore) GetInTaskChan() chan Request {
	return v.inTaskCh
}

func (v *VertexCore) SetInAckChan(ch chan Request) {
	v.inAckCh = ch
}

func (v *VertexCore) GetInAckChan() chan Request {
	return v.inAckCh
}

func (v *VertexCore) ReqSanityCheck(typ constants.RequestType) error {
	if typ == constants.RequestType_OnRecv {
		if v.FuncOnRecv == nil {
			return errors.New("hook has not set up OnRecv function")
		}
	} else if typ == constants.RequestType_OnNotify {
		if v.FuncOnNotify == nil {
			return errors.New("hook has not set up OnNotify function")
		}
	} else {
		return errors.New("invalid request type")
	}

	return nil
}

// Handle runs a function that has been registered by On().
func (v *VertexCore) Handle(req *Request) error {
	typ := req.Typ

	// Check if the function is already registered.
	if err := v.ReqSanityCheck(typ); err != nil {
		return err
	}

	ts := req.Ts
	e := req.Edge
	m := req.Msg

	// Handle timestamp here when doing OnRecv or OnNotify.
	// This is for Ingress, Egress and Feedback vertices.
	newTs := timestamp.CopyTimestampFrom(&ts)
	timestamp.HandleTimestamp(v.typ, newTs)

	// Trigger the request for next data step.
	if typ == constants.RequestType_OnRecv {
		v.FuncOnRecv(e, m, ts)
	} else if typ == constants.RequestType_OnNotify {
		v.FuncOnNotify(ts)
	}

	// Handle things internally after triggering request.
	v.PostFn(typ, e, &ts)

	return nil
}

func (v *VertexCore) PreFn(
	e Edge,
	ts *timestamp.Timestamp,
) {
	// According to the paper, in PreFn, there are two things to do with OC:
	// When doing SendBy, OC[(t, e)] <- OC[(t, e)] + 1.
	// When doing NotifyAt, OC[(t, v)] <- OC[(t, v)] + 1.
	v.extCh <- Request{
		Typ:  constants.RequestType_IncreOC,
		Edge: e,
		Ts:   *ts,
		Msg:  Message{},
	}
	select {
	case <-v.ctx.Done():
		return
	case <-v.inAckCh: // TODO maybe error?
		return
	}

}

func (v *VertexCore) PostFn(
	typ constants.RequestType,
	e Edge,
	ts *timestamp.Timestamp,
) {

	// If the vertex is an input vertex, then the OnRecv is triggered
	// an external data source.
	// In this case we don't change OC in the graph.
	if typ == constants.RequestType_OnRecv &&
		v.GetType() == constants.VertexType_Input {
		return
	}

	// If the vertex is an input vertex, then the OnRecv call should be triggered
	// by external data source. Therefore
	// According to the paper, in PostFn, there are two things to do with OC:
	// When doing OnRecv, OC[(t, e)] <- OC[(t, e)] - 1
	// When doing OnNotify, OC[(t, v)] <- OC[(t, v)] - 1
	if typ == constants.RequestType_OnRecv ||
		typ == constants.RequestType_OnNotify {
		v.extCh <- Request{
			Typ:  constants.RequestType_DecreOC,
			Edge: e,
			Ts:   *ts,
			Msg:  Message{},
		}
		select {
		case <-v.ctx.Done():
			return
		case <-v.inAckCh: // TODO maybe error?
			return
		}
	}
}

func (v *VertexCore) SendBy(e Edge, m Message, ts timestamp.Timestamp) error {
	ch := v.extCh
	if ch == nil {
		return errors.New("cannot do SendBy because extCh not set up")
	}

	// Handle things internally before triggering request.
	v.PreFn(e, &ts)

	ch <- Request{
		Typ:  constants.RequestType_SendBy,
		Edge: e,
		Ts:   ts,
		Msg:  m,
	}
	return nil

}

func (v *VertexCore) NotifyAt(ts timestamp.Timestamp) error {
	ch := v.extCh
	if ch == nil {
		return errors.New("cannot do NotifyAt because extCh not set up")
	}

	id := v.GetId()
	edge := NewEdge(id, id)

	// Handle things internally before triggering request.
	v.PreFn(edge, &ts)

	ch <- Request{
		Typ:  constants.RequestType_NotifyAt,
		Edge: edge, // Since NotifyAt is calling at a vertex itself, just set the edge to be itself.
		Ts:   ts,
		Msg:  Message{},
	}
	return nil
}

func (v *VertexCore) OnRecv(f func(e Edge, m Message, ts timestamp.Timestamp) error) {
	v.FuncOnRecv = f
}

func (v *VertexCore) OnNotify(f func(ts timestamp.Timestamp) error) {
	v.FuncOnNotify = f
}
