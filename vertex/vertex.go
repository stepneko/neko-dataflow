package vertex

import (
	"context"
	"errors"
	"fmt"
)

type VertexType int

const (
	VertexType_Generic  VertexType = 1
	VertexType_Ingress  VertexType = 2
	VertexType_Egress   VertexType = 3
	VertexType_Feedback VertexType = 4
	VertexType_Input    VertexType = 5
)

// Vertex is the interface that represents a vertex in the computing graph.
type Vertex interface {
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
	// On registers callbacks with types to the vertex.
	On(typ CallbackType, cb Callback) error
	// Handle triggers the callbacks with request.
	Handle(ctx context.Context, req *Request) error
	// HandleTimestamp handles timestamp since the vertex could be ingress, egress or feedback.
	HandleTimestamp(ts *Timestamp) error
	// Start starts a runtime for the vertex to handle dataflow.
	Start(ctx context.Context) error
}

type GenericVertex struct {
	vertexType VertexType
	extCh      chan Request
	inTaskCh   chan Request
	inAckCh    chan Request
	callbacks  map[CallbackType]Callback
}

func NewGenericVertex() *GenericVertex {
	return &GenericVertex{
		vertexType: VertexType_Generic,
		extCh:      nil,
		inTaskCh:   nil,
		inAckCh:    nil,
		callbacks:  make(map[CallbackType]Callback),
	}
}

func (v *GenericVertex) Start(ctx context.Context) error {
	for {
		select {
		case req := <-v.GetInTaskChan():
			v.Handle(ctx, &req)
		case <-ctx.Done():
			return nil
		}
	}
}

func (v *GenericVertex) SetExtChan(ch chan Request) {
	v.extCh = ch
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

// Register a function to this VertexCore
func (v *GenericVertex) On(typ CallbackType, cb Callback) error {
	if _, exist := v.callbacks[typ]; exist {
		return errors.New(fmt.Sprintf("function type %d already registered", typ))
	}
	v.callbacks[typ] = cb
	return nil
}

// Handle runs a function that has been registered by On()
func (v *GenericVertex) Handle(ctx context.Context, req *Request) error {
	typ := req.GetType()
	ts := req.GetTimestamp()
	e := req.GetEdge()
	m := req.GetMessage()

	fn, exist := v.callbacks[typ]
	if !exist {
		return errors.New(fmt.Sprintf("function type %d not registered by triggered", typ))
	}

	// Handle things internally before triggering callback
	v.PreFn(ctx, typ, e, &ts)

	// Trigger the callback
	fn(e, m, ts)

	// Handle things internally after triggering callback
	v.PostFn(ctx, typ, e, &ts)

	return nil
}

func (v GenericVertex) HandleTimestamp(ts *Timestamp) error {
	typ := v.vertexType
	if typ == VertexType_Ingress {
		ts.counters = append(ts.counters, 0)
	} else if typ == VertexType_Egress {
		l := len(ts.counters)
		if l == 0 {
			return errors.New("timestamp handling error in egress vertex. Counter already empty so cannot pop")
		}
		ts.counters = ts.counters[:l-1]
	} else if typ == VertexType_Feedback {
		l := len(ts.counters)
		if l == 0 {
			return errors.New("timestamp handling error in feedback vertex. Counter already empty")
		}
		ts.counters[l-1] += 1
	}
	return nil
}

func (v *GenericVertex) PreFn(
	ctx context.Context,
	typ CallbackType,
	e Edge,
	ts *Timestamp,
) {
	// According to the paper, in PreFn, there are two things to do with OC:
	// When doing SendBy, OC[(t, e)] <- OC[(t, e)] + 1
	// When doing NotifyAt, OC[(t, v)] <- OC[(t, v)] + 1
	if typ == CallbackType_SendBy || typ == CallbackType_NotifyAt {
		v.extCh <- *NewRequest(
			CallbackType_IncreOC,
			e,
			*ts,
			Message{},
		)
		select {
		case <-ctx.Done():
			return
		case <-v.inAckCh: // TODO maybe error?
			return
		}

	}

	// Handle timestamp here when doing OnRecv or OnNotify
	if typ == CallbackType_OnRecv || typ == CallbackType_OnNotify {
		v.HandleTimestamp(ts)
	}
}

func (v *GenericVertex) PostFn(
	ctx context.Context,
	typ CallbackType,
	e Edge,
	ts *Timestamp,
) {
	// According to the paper, in PostFn, there are two things to do with OC:
	// When doing OnRecv, OC[(t, e)] <- OC[(t, e)] - 1
	// When doing OnNotify, OC[(t, v)] <- OC[(t, v)] - 1
	if typ == CallbackType_OnRecv || typ == CallbackType_OnNotify {
		v.extCh <- *NewRequest(
			CallbackType_DecreOC,
			e,
			*ts,
			Message{},
		)
		select {
		case <-ctx.Done():
			return
		case <-v.inAckCh: // TODO maybe error?
			return
		}
	}
}
