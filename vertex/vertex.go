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
	// Set Id of the vertex.
	SetId(int)
	// Get Id of the vertex.
	GetId() int
	// Get Type of the vertex.
	GetType() constants.VertexType
	// SetExtChan sets the chan handle sending requests to scheduler.
	SetExtChan(chan Request)
	// GetExtChan gets the chan handle sending requests to scheduler.
	GetExtChan() chan Request
	// SetInTaskChan sets the chan handle receiving task requests from scheduler.
	SetInTaskChan(chan Request)
	// GetInTaskChan gets the chan handle receiving task requests from scheduler.
	GetInTaskChan() chan Request
	// SetInAckChan sets the chan handle receiving acks from scheduler.
	SetInAckChan(chan Request)
	// GetInAckChan gets the chan handle receiving acks from scheduler.
	GetInAckChan() chan Request
	// Handle triggers the processing of request.
	Handle(ctx context.Context, req *Request) error
	// SendBy is a wrapper that triggers the RequestType_SendBy function
	SendBy(e Edge, m Message, ts timestamp.Timestamp) error
	// NotifyAt is a wrapper that triggers the RequestType_NotifyAt function
	NotifyAt(ts timestamp.Timestamp) error
	// OnRecv registers RequestType_OnRecv function
	OnRecv(f func(e Edge, m Message, ts timestamp.Timestamp) error)
	// OnNotify registers RequestType_OnNotify function
	OnNotify(f func(ts timestamp.Timestamp) error)
	// Start starts a runtime for the vertex to handle dataflow.
	Start(ctx context.Context, wg sync.WaitGroup) error
}

type VertexFunctionHook struct {
	SendBy   func(e Edge, m Message, ts timestamp.Timestamp) error
	NotifyAt func(v Vertex, ts timestamp.Timestamp) error
	OnRecv   func(e Edge, m Message, ts timestamp.Timestamp) error
	OnNotify func(ts timestamp.Timestamp) error
}

func (h *VertexFunctionHook) SetupExtChan(ch chan Request) {
	h.SendBy = func(e Edge, m Message, ts timestamp.Timestamp) error {
		ch <- Request{
			Typ:  constants.RequestType_SendBy,
			Edge: e,
			Ts:   ts,
			Msg:  m,
		}
		return nil
	}

	h.NotifyAt = func(v Vertex, ts timestamp.Timestamp) error {
		ch <- Request{
			Typ:  constants.RequestType_NotifyAt,
			Edge: NewEdge(v, v), // Since NotifyAt is calling at a vertex itself, just set the edge to be itself.
			Ts:   ts,
			Msg:  Message{},
		}
		return nil
	}
}

func (h *VertexFunctionHook) SanityCheck(typ constants.RequestType) error {
	if typ == constants.RequestType_SendBy {
		if h.SendBy == nil {
			return errors.New("hook has not set up SendBy function")
		}
	} else if typ == constants.RequestType_NotifyAt {
		if h.NotifyAt == nil {
			return errors.New("hook has not set up NotifyAt function")
		}
	} else if typ == constants.RequestType_OnRecv {
		if h.OnRecv == nil {
			return errors.New("hook has not set up OnRecv function")
		}
	} else if typ == constants.RequestType_OnNotify {
		if h.OnNotify == nil {
			return errors.New("hook has not set up OnNotify function")
		}
	} else {
		return errors.New("invalid request type")
	}

	return nil
}

type GenericVertex struct {
	id       int
	typ      constants.VertexType
	extCh    chan Request
	inTaskCh chan Request
	inAckCh  chan Request
	hook     VertexFunctionHook
}

func NewGenericVertex() *GenericVertex {
	return &GenericVertex{
		id:       0,
		typ:      constants.VertexType_Generic,
		extCh:    nil,
		inTaskCh: nil,
		inAckCh:  nil,
		hook:     VertexFunctionHook{},
	}
}

func (v *GenericVertex) Start(ctx context.Context, wg sync.WaitGroup) error {
	defer wg.Done()
	for {
		select {
		case req := <-v.GetInTaskChan():
			v.Handle(ctx, &req)
		case <-ctx.Done():
			return nil
		}
	}
}

func (v *GenericVertex) SetId(id int) {
	v.id = id
}

func (v *GenericVertex) GetId() int {
	return v.id
}

func (v *GenericVertex) GetType() constants.VertexType {
	return v.typ
}

func (v *GenericVertex) SetExtChan(ch chan Request) {
	v.extCh = ch
	v.hook.SetupExtChan(ch)
}

func (v *GenericVertex) GetExtChan() chan Request {
	return v.extCh
}

func (v *GenericVertex) SetInTaskChan(ch chan Request) {
	v.inTaskCh = ch
}

func (v *GenericVertex) GetInTaskChan() chan Request {
	return v.inTaskCh
}

func (v *GenericVertex) SetInAckChan(ch chan Request) {
	v.inAckCh = ch
}

func (v *GenericVertex) GetInAckChan() chan Request {
	return v.inAckCh
}

// Handle runs a function that has been registered by On().
func (v *GenericVertex) Handle(ctx context.Context, req *Request) error {
	typ := req.Typ

	// Check if the function is already registered.
	if err := v.hook.SanityCheck(typ); err != nil {
		return err
	}

	ts := req.Ts
	e := req.Edge
	m := req.Msg

	// Handle things internally before triggering request.
	v.PreFn(ctx, typ, e, &ts)

	// Handle timestamp here when doing OnRecv or OnNotify.
	// This is for Ingress, Egress and Feedback vertices.
	newTs := timestamp.CopyTimestampFrom(&ts)
	if typ == constants.RequestType_OnRecv ||
		typ == constants.RequestType_OnNotify {
		timestamp.HandleTimestamp(v.typ, newTs)
	}

	// Trigger the request for next data step.
	if typ == constants.RequestType_SendBy {
		v.hook.SendBy(e, m, ts)
	} else if typ == constants.RequestType_NotifyAt {
		v.hook.NotifyAt(v, ts)
	} else if typ == constants.RequestType_OnRecv {
		v.hook.OnRecv(e, m, ts)
	} else if typ == constants.RequestType_OnNotify {
		v.hook.OnNotify(ts)
	}

	// Handle things internally after triggering request.
	v.PostFn(ctx, typ, e, &ts)

	return nil
}

func (v *GenericVertex) PreFn(
	ctx context.Context,
	typ constants.RequestType,
	e Edge,
	ts *timestamp.Timestamp,
) {
	// According to the paper, in PreFn, there are two things to do with OC:
	// When doing SendBy, OC[(t, e)] <- OC[(t, e)] + 1.
	// When doing NotifyAt, OC[(t, v)] <- OC[(t, v)] + 1.
	if typ == constants.RequestType_SendBy ||
		typ == constants.RequestType_NotifyAt {
		v.extCh <- Request{
			Typ:  constants.RequestType_IncreOC,
			Edge: e,
			Ts:   *ts,
			Msg:  Message{},
		}
		select {
		case <-ctx.Done():
			return
		case <-v.inAckCh: // TODO maybe error?
			return
		}

	}
}

func (v *GenericVertex) PostFn(
	ctx context.Context,
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
		case <-ctx.Done():
			return
		case <-v.inAckCh: // TODO maybe error?
			return
		}
	}
}

func (v *GenericVertex) SendBy(e Edge, m Message, ts timestamp.Timestamp) error {
	return v.hook.SendBy(e, m, ts)
}

func (v *GenericVertex) NotifyAt(ts timestamp.Timestamp) error {
	return v.hook.NotifyAt(v, ts)
}

func (v *GenericVertex) OnRecv(f func(e Edge, m Message, ts timestamp.Timestamp) error) {
	v.hook.OnRecv = f
}

func (v *GenericVertex) OnNotify(f func(ts timestamp.Timestamp) error) {
	v.hook.OnNotify = f
}
