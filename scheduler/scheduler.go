package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/vertex"
	"go.uber.org/zap"
)

type Scheduler interface {
	CreateVertexId() constants.VertexId
	RegisterVertex(v vertex.Vertex) error
	BuildEdge() error
	Step() error
	Run() error
}

type SimpleScheduler struct {
	nextId constants.VertexId
	ch     chan vertex.Request
	graph  *Graph
}

// NewScheduler returns a simple scheduler of neko-dataflow
func NewScheduler() *SimpleScheduler {
	return &SimpleScheduler{
		nextId: 0,
		ch:     make(chan vertex.Request, 1024),
		graph:  NewGraph(),
	}
}

func (s *SimpleScheduler) CreateVertexId() constants.VertexId {
	id := s.nextId
	s.nextId += 1
	return id
}

func (s *SimpleScheduler) RegisterVertex(v vertex.Vertex) {
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

	// Insert the vertex into scheduler
	s.graph.InsertVertex(v)
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

	e := vertex.NewEdge(src.GetId(), target.GetId())
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
	for id := range s.graph.VertexMap {
		wg.Add(1)
		v := s.graph.VertexMap[id].vertex
		go v.Start(ctx, wg)
	}

	go s.Serve(ctx)
	wg.Wait()

	return nil
}

func (s *SimpleScheduler) Serve(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-s.ch:
			if err := s.HandleReq(&req); err != nil {
				zap.L().Error(err.Error())
			}
		}
	}
}

func (s *SimpleScheduler) HandleReq(req *vertex.Request) error {
	typ := req.Typ

	if typ == constants.RequestType_IncreOC {
		s.IncreOC(req)
	} else if typ == constants.RequestType_DecreOC {
		s.DecreOC(req)
	} else if typ == constants.RequestType_SendBy {
		s.SendBy(req)
	} else if typ == constants.RequestType_NotifyAt {
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
	node, exist := s.graph.VertexMap[e.GetSrc()]
	if !exist {
		return errors.New(fmt.Sprintf("vertex not found when doing IncreOC with id: %d", e.GetSrc()))
	}
	s.graph.IncreOC(ps)
	node.vertex.GetInAckChan() <- vertex.Request{
		Typ:  constants.RequestType_Ack,
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
	node, exist := s.graph.VertexMap[e.GetTarget()]
	if !exist {
		return errors.New(fmt.Sprintf("vertex not found when doing DecreOC with id: %d", e.GetTarget()))
	}
	s.graph.DecreOC(ps)
	node.vertex.GetInAckChan() <- vertex.Request{
		Typ:  constants.RequestType_Ack,
		Edge: nil,
		Ts:   timestamp.Timestamp{},
		Msg:  vertex.Message{},
	}
	return nil
}

func (s *SimpleScheduler) SendBy(req *vertex.Request) error {
	e := req.Edge
	target := e.GetTarget()
	node, exist := s.graph.VertexMap[target]
	if !exist {
		return errors.New(fmt.Sprintf("vertex not found when doing SendBy with id: %d", target))
	}
	ch := node.vertex.GetInTaskChan()
	ch <- vertex.Request{
		Typ:  constants.RequestType_OnRecv,
		Edge: e,
		Ts:   req.Ts,
		Msg:  req.Msg,
	}
	return nil
}

func (s *SimpleScheduler) NotifyAt(req *vertex.Request) error {
	e := req.Edge
	target := e.GetTarget()
	node, exist := s.graph.VertexMap[target]
	if !exist {
		return errors.New(fmt.Sprintf("vertex not found with doing NotifyAt with id: %d", target))
	}
	node.vertex.GetInTaskChan() <- vertex.Request{
		Typ:  constants.RequestType_OnNotify,
		Edge: e,
		Ts:   req.Ts,
		Msg:  req.Msg,
	}
	return nil
}
