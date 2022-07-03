package scheduler

import (
	"context"
	"errors"
	"sync"

	"github.com/stepneko/neko-dataflow/vertex"
)

type Scheduler interface {
	RegisterVertex(v vertex.Vertex) error
	BuildEdge() error
	Step() error
	Run() error
}

type SimpleScheduler struct {
	ch        chan vertex.Request
	vertexMap map[vertex.Vertex]*VertexStatus
	frontier  []vertex.Vertex
	graph     *Graph
}

// NewScheduler returns a simple scheduler of neko-dataflow
func NewScheduler() *SimpleScheduler {
	return &SimpleScheduler{
		ch:        make(chan vertex.Request, 1024),
		vertexMap: make(map[vertex.Vertex]*VertexStatus),
		frontier:  []vertex.Vertex{},
		graph:     NewGraph(),
	}
}

func (s *SimpleScheduler) RegisterVertex(v vertex.Vertex) {
	// Insert the vertex into scheduler
	s.graph.InsertVertex(v)
	// Set up channel into scheduler
	v.SetExtChan(s.ch)
	v.On(vertex.CallbackType_SendBy, func(e vertex.Edge, msg vertex.Message, ts vertex.Timestamp) error {
		v.GetExtChan() <- *vertex.NewRequest(
			vertex.CallbackType_SendBy,
			e,
			ts,
			msg,
		)
		return nil
	})

	v.On(vertex.CallbackType_NotifyAt, func(e vertex.Edge, msg vertex.Message, ts vertex.Timestamp) error {
		v.GetExtChan() <- *vertex.NewRequest(
			vertex.CallbackType_NotifyAt,
			vertex.NewEdge(v, v), // Since NotifyAt is calling at a vertex itself, just set the edge to be itself.
			ts,
			msg,
		)
		return nil
	})

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

func (s *SimpleScheduler) Step() error {
	return nil
}

// Run starts the scheduler after all vertices are registered
// This also gives scheduler an opportunity to optimize the graph
func (s *SimpleScheduler) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for v := range s.vertexMap {
		wg.Add(1)
		go v.Start(ctx)
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
	typ := req.GetType()

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
	ts := req.GetTimestamp()
	var ps vertex.Pointstamp
	e := req.GetEdge()
	if e.GetSrc() == e.GetTarget() {
		ps = vertex.NewVertexPointStamp(
			e.GetSrc(),
			&ts,
		)
	} else {
		ps = vertex.NewEdgePointStamp(
			e,
			&ts,
		)
	}
	s.graph.IncreOC(ps)
	s.vertexMap[e.GetSrc()].ackChan <- *vertex.NewRequest(
		vertex.CallbackType_Ack,
		nil,
		vertex.Timestamp{},
		vertex.Message{},
	)
	return nil
}

func (s *SimpleScheduler) DecreOC(req *vertex.Request) error {
	ts := req.GetTimestamp()
	var ps vertex.Pointstamp
	e := req.GetEdge()
	if e.GetSrc() == e.GetTarget() {
		ps = vertex.NewVertexPointStamp(
			e.GetSrc(),
			&ts,
		)
	} else {
		ps = vertex.NewEdgePointStamp(
			e,
			&ts,
		)
	}
	s.graph.DecreOC(ps)
	s.vertexMap[e.GetSrc()].ackChan <- *vertex.NewRequest(
		vertex.CallbackType_Ack,
		nil,
		vertex.Timestamp{},
		vertex.Message{},
	)
	return nil
}

func (s *SimpleScheduler) SendBy(req *vertex.Request) error {
	e := req.GetEdge()
	target := e.GetTarget()
	ch := s.vertexMap[target]
	ch.taskChan <- *vertex.NewRequest(
		vertex.CallbackType_OnRecv,
		e,
		req.GetTimestamp(),
		req.GetMessage(),
	)
	return nil
}

func (s *SimpleScheduler) NotifyAt(req *vertex.Request) error {
	e := req.GetEdge()
	target := e.GetTarget()
	ch := s.vertexMap[target]
	ch.taskChan <- *vertex.NewRequest(
		vertex.CallbackType_OnNotify,
		e,
		req.GetTimestamp(),
		req.GetMessage(),
	)
	return nil
}
