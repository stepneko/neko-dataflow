package scheduler

import (
	"context"
	"errors"
	"sync"

	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/vertex"
)

type Scheduler interface {
	CreateVertexId() int
	RegisterVertex(v vertex.Vertex) error
	BuildEdge() error
	Step() error
	Run() error
}

type SimpleScheduler struct {
	nextId    int
	ch        chan vertex.Request
	vertexMap map[vertex.Vertex]*VertexStatus
	frontier  []vertex.Vertex
	graph     *Graph
}

// NewScheduler returns a simple scheduler of neko-dataflow
func NewScheduler() *SimpleScheduler {
	return &SimpleScheduler{
		nextId:    0,
		ch:        make(chan vertex.Request, 1024),
		vertexMap: make(map[vertex.Vertex]*VertexStatus),
		frontier:  []vertex.Vertex{},
		graph:     NewGraph(),
	}
}

func (s *SimpleScheduler) CreateVertexId() int {
	id := s.nextId
	s.nextId += 1
	return id
}

func (s *SimpleScheduler) RegisterVertex(v vertex.Vertex) {
	// Insert the vertex into scheduler
	s.graph.InsertVertex(v)
	// Set up id by scheduler
	v.SetId(s.CreateVertexId())
	// Set up channel into scheduler
	// By doing this, the hook of the vertex is also set up
	// with the channel, so that the SendBy and NotifyAt is
	// wired up with the scheduler.
	v.SetExtChan(s.ch)

	// Set up vertex status in scheduler and channels in vertex
	taskChan := make(chan vertex.Request, 1024)
	v.SetInTaskChan(taskChan)

	// Set up ack channel from scheduler to vertex
	ackChan := make(chan vertex.Request, 1024)
	v.SetInAckChan(ackChan)

	s.vertexMap[v] = NewVertexStatus(
		taskChan,
		ackChan,
	)
}

func (s *SimpleScheduler) BuildEdge(
	src vertex.Vertex,
	target vertex.Vertex,
) (vertex.Edge, error) {
	if src == nil {
		return nil, errors.New("src vertex cannot be nil")
	}
	if target == nil {
		return nil, errors.New("target vertex connot be nil")
	}

	e := vertex.NewEdge(src, target)
	s.graph.InsertEdge(e)

	return e, nil
}

// TODO Step is for finer control of scheduler.
func (s *SimpleScheduler) Step() error {
	return nil
}

// Run starts the scheduler after all vertices are registered.
// This also pre-processes and optimizes the graph.
func (s *SimpleScheduler) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.graph.PreProcess()

	var wg sync.WaitGroup
	for v := range s.vertexMap {
		wg.Add(1)
		go v.Start(ctx, wg)
	}

	go s.Serve(ctx)
	wg.Wait()

	return nil
}

func (s *SimpleScheduler) Serve(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case req := <-s.ch:
			if err := s.HandleReq(&req); err != nil {
				return nil
			}
		}
	}
}

func (s *SimpleScheduler) HandleReq(req *vertex.Request) error {
	typ := req.Typ

	if typ == vertex.CallbackType_IncreOC {
		s.IncreOC(req)
	} else if typ == vertex.CallbackType_DecreOC {
		s.DecreOC(req)
	} else if typ == vertex.CallbackType_SendBy {
		s.SendBy(req)
	} else if typ == vertex.CallbackType_NotifyAt {
		s.NotifyAt(req)
	}
	return nil
}

func (s *SimpleScheduler) IncreOC(req *vertex.Request) error {
	ts := req.Ts
	var ps Pointstamp
	e := req.Edge
	if e.GetSrc() == e.GetTarget() {
		ps = NewVertexPointStamp(
			e.GetSrc(),
			&ts,
		)
	} else {
		ps = NewEdgePointStamp(
			e,
			&ts,
		)
	}
	s.graph.IncreOC(ps)
	s.vertexMap[e.GetSrc()].ackChan <- vertex.Request{
		Typ:  vertex.CallbackType_Ack,
		Edge: nil,
		Ts:   timestamp.Timestamp{},
		Msg:  vertex.Message{},
	}
	return nil
}

func (s *SimpleScheduler) DecreOC(req *vertex.Request) error {
	ts := req.Ts
	var ps Pointstamp
	e := req.Edge
	if e.GetSrc() == e.GetTarget() {
		ps = NewVertexPointStamp(
			e.GetSrc(),
			&ts,
		)
	} else {
		ps = NewEdgePointStamp(
			e,
			&ts,
		)
	}
	s.graph.DecreOC(ps)
	s.vertexMap[e.GetTarget()].ackChan <- vertex.Request{
		Typ:  vertex.CallbackType_Ack,
		Edge: nil,
		Ts:   timestamp.Timestamp{},
		Msg:  vertex.Message{},
	}
	return nil
}

func (s *SimpleScheduler) SendBy(req *vertex.Request) error {
	e := req.Edge
	target := e.GetTarget()
	ch := s.vertexMap[target]
	ch.taskChan <- vertex.Request{
		Typ:  vertex.CallbackType_OnRecv,
		Edge: e,
		Ts:   req.Ts,
		Msg:  req.Msg,
	}
	return nil
}

func (s *SimpleScheduler) NotifyAt(req *vertex.Request) error {
	e := req.Edge
	target := e.GetTarget()
	ch := s.vertexMap[target]
	ch.taskChan <- vertex.Request{
		vertex.CallbackType_OnNotify,
		e,
		req.Ts,
		req.Msg,
	}
	return nil
}
