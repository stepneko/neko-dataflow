package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/graph"
	"github.com/stepneko/neko-dataflow/handles"
	"github.com/stepneko/neko-dataflow/request"
	"github.com/stepneko/neko-dataflow/scope"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/vertex"
)

type DataflowFunc = func(s scope.Scope) error

// Worker is a worker unit containing a dataflow graph,
// taking inputs and making computations.
type Worker interface {
	Dataflow(fn DataflowFunc) error
	ToScope() scope.Scope
	Run() error
}

type SimpleWorker struct {
	ctx        context.Context
	id         constants.WorkerId
	vidFactory vertex.IdFactory
	graph      *graph.Graph
	handle     handles.WorkerHandle
	vHandles   map[constants.VertexId]map[constants.VertexId]handles.VertexHandle
	vertices   map[constants.VertexId]vertex.Vertex
}

func NewSimpleWorker(ctx context.Context) *SimpleWorker {
	return &SimpleWorker{
		ctx:        ctx,
		id:         0,
		vidFactory: vertex.NewSimpleIdFactory(),
		graph:      graph.NewGraph(),
		handle:     handles.NewSimpleWorkerHandle(),
		vHandles:   make(map[constants.VertexId]map[constants.VertexId]handles.VertexHandle),
		vertices:   make(map[constants.VertexId]vertex.Vertex),
	}
}

//============== Impl Worker interface ================//

// Dataflow builds the dataflow as described in the function fn.
// Registers vertices and build edges in running the function.
func (w *SimpleWorker) Dataflow(fn DataflowFunc) error {
	if err := fn(w.ToScope()); err != nil {
		return err
	}
	return nil
}

// Currently worker implements both Worker and Scope interfaces
// and this works pretty fine. This ToScope() is a place holder
// function for future use if we want to separate scope from worker.
func (w *SimpleWorker) ToScope() scope.Scope {
	return w
}

func (w *SimpleWorker) Run() error {

	w.graph.PreProcess()

	var wg sync.WaitGroup
	for id := range w.vertices {
		wg.Add(1)
		v := w.vertices[id]
		go v.Start(wg)
	}

	wg.Add(1)
	go w.serve(wg)

	wg.Wait()

	return nil
}

//============== Impl Scope interface ================//
func (w *SimpleWorker) Name() string {
	return fmt.Sprintf("worker %d", w.id)
}

func (w *SimpleWorker) GenerateVID() constants.VertexId {
	return w.vidFactory.Generate()
}

func (w *SimpleWorker) GetWorkerHandle() handles.WorkerHandle {
	return w.handle
}

func (w *SimpleWorker) RegisterVertex(
	v vertex.Vertex,
	handle handles.VertexHandle,
) error {
	if v == nil {
		return errors.New("vertex cannot be nil")
	}

	vid := v.Id()
	w.vertices[vid] = v

	w.setHandle(vid, vid, handle)

	// Insert the vertex into scheduler
	w.graph.InsertVertex(vid, v.Type())
	return nil
}

func (w *SimpleWorker) RegisterEdge(
	src vertex.Vertex,
	target vertex.Vertex,
	handle handles.VertexHandle,
) error {
	if src == nil {
		return errors.New("src vertex cannot be nil")
	}
	if target == nil {
		return errors.New("target vertex connot be nil")
	}

	srcId := src.Id()
	targetId := target.Id()

	w.setHandle(srcId, targetId, handle)

	e := edge.NewEdge(srcId, targetId)
	w.graph.InsertEdge(e)

	return nil
}

func (w *SimpleWorker) Done() <-chan struct{} {
	return w.ctx.Done()
}

//============== Private functions ================//
func (w *SimpleWorker) getHandle(
	src constants.VertexId,
	target constants.VertexId,
) (handles.VertexHandle, error) {
	m, exist := w.vHandles[src]
	if !exist {
		return nil, fmt.Errorf("cannot find handle becasue src not found with id %d", src)
	}
	h, exist := m[target]
	if !exist {
		return nil, fmt.Errorf("cannot find handle because target not found with id %d", target)
	}
	return h, nil
}

func (w *SimpleWorker) setHandle(
	src constants.VertexId,
	target constants.VertexId,
	handle handles.VertexHandle,
) {
	_, exist := w.vHandles[src]
	if !exist {
		w.vHandles[src] = make(map[constants.VertexId]handles.VertexHandle)
	}
	w.vHandles[src][target] = handle
}

func (w *SimpleWorker) handleReq(req *request.Request) error {
	typ := req.Typ

	if typ == constants.RequestType_IncreOC {
		return w.increOC(req)
	} else if typ == constants.RequestType_DecreOC {
		return w.decreOC(req)
	} else if typ == constants.RequestType_SendBy {
		return w.sendBy(req)
	} else if typ == constants.RequestType_NotifyAt {
		return w.notifyAt(req)
	}
	return nil
}

func (w *SimpleWorker) serve(wg sync.WaitGroup) error {
	defer wg.Done()
	ch := w.handle.Recv()
	for {
		select {
		case <-w.ctx.Done():
			return nil
		case req := <-ch:
			if err := w.handleReq(&req); err != nil {
				return err
			}
		}
	}
}

func (w *SimpleWorker) increOC(req *request.Request) error {
	ts := req.Ts
	var ps graph.Pointstamp
	e := req.Edge
	if e.GetSrc() == e.GetTarget() {
		ps = graph.NewVertexPointStamp(
			e.GetSrc(),
			&ts,
		)
	} else {
		ps = graph.NewEdgePointStamp(
			e,
			&ts,
		)
	}
	if err := w.graph.IncreOC(ps); err != nil {
		return err
	}
	vid := e.GetSrc()
	vHandle, err := w.getHandle(vid, vid)
	if err != nil {
		return err
	}
	newReq := request.Request{
		Typ:  constants.RequestType_Ack,
		Edge: nil,
		Ts:   timestamp.Timestamp{},
		Msg:  request.Message{},
	}
	vHandle.Ack(&newReq)
	return nil
}

func (w *SimpleWorker) decreOC(req *request.Request) error {
	ts := req.Ts
	var ps graph.Pointstamp
	e := req.Edge
	if e.GetSrc() == e.GetTarget() {
		ps = graph.NewVertexPointStamp(
			e.GetSrc(),
			&ts,
		)
	} else {
		ps = graph.NewEdgePointStamp(
			e,
			&ts,
		)
	}
	if err := w.graph.DecreOC(ps); err != nil {
		return err
	}
	vid := e.GetTarget()
	vHandle, err := w.getHandle(vid, vid)
	if err != nil {
		return err
	}
	newReq := request.Request{
		Typ:  constants.RequestType_Ack,
		Edge: nil,
		Ts:   timestamp.Timestamp{},
		Msg:  request.Message{},
	}
	vHandle.Ack(&newReq)
	return nil
}

func (w *SimpleWorker) sendBy(req *request.Request) error {
	e := req.Edge
	src := e.GetSrc()
	target := e.GetTarget()
	vHandle, err := w.getHandle(src, target)
	if err != nil {
		return err
	}
	newReq := request.Request{
		Typ:  constants.RequestType_OnRecv,
		Edge: e,
		Ts:   req.Ts,
		Msg:  req.Msg,
	}
	vHandle.Send(&newReq)
	return nil
}

func (w *SimpleWorker) notifyAt(req *request.Request) error {
	e := req.Edge
	target := e.GetTarget()
	vHandle, err := w.getHandle(target, target)
	if err != nil {
		return err
	}
	newReq := request.Request{
		Typ:  constants.RequestType_OnNotify,
		Edge: e,
		Ts:   req.Ts,
		Msg:  req.Msg,
	}
	vHandle.Send(&newReq)
	return nil
}
