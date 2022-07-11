package vertex

import (
	"context"
	"errors"
	"fmt"
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
	SetInTaskChans(chs [constants.VertexInDirs]chan Request)
	// GetInTaskChan gets the chan handle receiving task requests from scheduler.
	GetInTaskChans() [constants.VertexInDirs]chan Request
	// SetInAckChan sets the chan handle receiving acks from scheduler.
	SetInAckChan(ch chan Request)
	// GetInAckChan gets the chan handle receiving acks from scheduler.
	GetInAckChan() chan Request
	// Start starts a runtime for the vertex to handle dataflow.
	Start(wg sync.WaitGroup) error
	// Handle triggers the processing of request.
	Handle(req *Request, dir constants.VertexInDir) error
	// SendBy is a wrapper that triggers the RequestType_SendBy function
	SendBy(e Edge, m Message, ts timestamp.Timestamp) error
	// NotifyAt is a wrapper that triggers the RequestType_NotifyAt function
	NotifyAt(ts timestamp.Timestamp) error
	// internalOnRecv registers RequestType_OnRecv function
	internalOnRecv(f func(e Edge, m Message, ts timestamp.Timestamp) error, dir constants.VertexInDir)
	// internalOnNotify registers RequestType_OnNotify function
	internalOnNotify(f func(ts timestamp.Timestamp) error, dir constants.VertexInDir)
}

type VertexCore struct {
	ctx     context.Context
	id      constants.VertexId
	typ     constants.VertexType
	currTs  timestamp.Timestamp
	extCh   chan Request
	inAckCh chan Request

	inTaskChs      [constants.VertexInDirs]chan Request
	FuncOnRecvs    [constants.VertexInDirs]func(e Edge, m Message, ts timestamp.Timestamp) error
	FuncOnNotifies [constants.VertexInDirs]func(ts timestamp.Timestamp) error
}

func NewVertexCore() *VertexCore {
	return &VertexCore{
		ctx:     nil,
		id:      0,
		typ:     constants.VertexType_Generic,
		extCh:   nil,
		inAckCh: nil,
		currTs:  *timestamp.NewTimestamp(),

		inTaskChs:      [constants.VertexInDirs]chan Request{},
		FuncOnRecvs:    [constants.VertexInDirs]func(e Edge, m Message, ts timestamp.Timestamp) error{},
		FuncOnNotifies: [constants.VertexInDirs]func(ts timestamp.Timestamp) error{},
	}
}

func (v *VertexCore) Start(wg sync.WaitGroup) error {
	defer wg.Done()
	for {
		select {
		case req := <-v.inTaskChs[constants.VertexInDir_Left]:
			v.Handle(&req, constants.VertexInDir_Left)
		case req := <-v.inTaskChs[constants.VertexInDir_Right]:
			v.Handle(&req, constants.VertexInDir_Right)
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

func (v *VertexCore) SetInTaskChans(chs [constants.VertexInDirs]chan Request) {
	v.inTaskChs = chs
}

func (v *VertexCore) GetInTaskChans() [constants.VertexInDirs]chan Request {
	return v.inTaskChs
}

func (v *VertexCore) SetInAckChan(ch chan Request) {
	v.inAckCh = ch
}

func (v *VertexCore) GetInAckChan() chan Request {
	return v.inAckCh
}

func (v *VertexCore) ReqSanityCheck(
	typ constants.RequestType,
	dir constants.VertexInDir,
) error {
	if typ == constants.RequestType_OnRecv {
		if v.FuncOnRecvs[dir] == nil {
			return errors.New(fmt.Sprintf("vertex has not set up OnRecv function on dir %d", dir))
		}
	} else if typ == constants.RequestType_OnNotify {
		if v.FuncOnNotifies[dir] == nil {
			return errors.New(fmt.Sprintf("vertex has not set up OnNotify function on dir %d", dir))
		}
	} else {
		return errors.New("invalid request type")
	}

	return nil
}

// Handle handles requests from directions of incoming task channels.
func (v *VertexCore) Handle(req *Request, dir constants.VertexInDir) error {
	typ := req.Typ

	// Check if the function is already registered.
	if err := v.ReqSanityCheck(typ, dir); err != nil {
		return err
	}

	ts := req.Ts
	e := req.Edge
	m := req.Msg

	v.currTs = ts

	// Handle timestamp here when doing OnRecv or OnNotify.
	// This is for Ingress, Egress and Feedback vertices.
	newTs := timestamp.CopyTimestampFrom(&ts)
	timestamp.HandleTimestamp(v.typ, newTs)

	// Trigger the request for next data step.
	if typ == constants.RequestType_OnRecv {
		v.FuncOnRecvs[dir](e, m, ts)
	} else if typ == constants.RequestType_OnNotify {
		v.FuncOnNotifies[dir](ts)
	}

	// Handle things internally after triggering request.
	v.PostFn(typ, e, &ts)

	return nil
}

func (v *VertexCore) PreFn(
	e Edge,
	ts *timestamp.Timestamp,
) error {

	// The OnRecv and OnNotify methods may contain
	// arbitrary code and modify arbitrary per-vertex state, but
	// do have an important constraint on their execution: when
	// invoked with a timestamp t, the methods may only call
	// SendBy or NotifyAt with times t′ ≥ t.
	// This rule guarantees that messages are not sent “backwards in time”
	if !timestamp.LE(&v.currTs, ts) {
		return errors.New("cannot do SendBy or NotifyAt to an earlier timestamp")
	}

	// Per the paper, in PreFn, there are two things to do with OC:
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
		return nil
	case <-v.inAckCh: // TODO maybe error?
		return nil
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
	if err := v.PreFn(e, &ts); err != nil {
		return err
	}

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
	if err := v.PreFn(edge, &ts); err != nil {
		return err
	}

	ch <- Request{
		Typ:  constants.RequestType_NotifyAt,
		Edge: edge, // Since NotifyAt is calling at a vertex itself, just set the edge to be itself.
		Ts:   ts,
		Msg:  Message{},
	}
	return nil
}

func (v *VertexCore) internalOnRecv(
	f func(e Edge, m Message, ts timestamp.Timestamp) error,
	dir constants.VertexInDir,
) {
	v.FuncOnRecvs[dir] = f
}

func (v *VertexCore) internalOnNotify(
	f func(ts timestamp.Timestamp) error,
	dir constants.VertexInDir,
) {
	v.FuncOnNotifies[dir] = f
}
