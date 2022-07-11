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
	ctx        context.Context
	cancelFunc context.CancelFunc
	nextId     constants.VertexId
	ch         chan vertex.Request
	graph      *Graph
}

// NewScheduler returns a simple scheduler of neko-dataflow
func NewScheduler() *SimpleScheduler {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &SimpleScheduler{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		nextId:     0,
		ch:         make(chan vertex.Request, 1024),
		graph:      NewGraph(),
	}
}

func (s *SimpleScheduler) CreateVertexId() constants.VertexId {
	id := s.nextId
	s.nextId += 1
	return id
}

func (s *SimpleScheduler) RegisterVertex(v vertex.Vertex) {
	// Set up context and id by scheduler
	v.SetContext(s.ctx)
	v.SetId(s.CreateVertexId())

	// Set up channel into scheduler
	// By doing this, the hook of the vertex is also set up
	// with the channel, so that the SendBy and NotifyAt is
	// wired up with the scheduler.
	v.SetExtChan(s.ch)

	// Set up vertex status in scheduler and channels in vertex
	taskChans := [constants.VertexInDirs]chan vertex.Request{}
	taskChans[constants.VertexInDir_Left] = make(chan vertex.Request, 1024)
	// If the vertex is a binary vertex, it has a taskChan2.
	if v.GetType() == constants.VertexType_Bianry {
		taskChans[constants.VertexInDir_Right] = make(chan vertex.Request, 1024)
	}
	v.SetInTaskChans(taskChans)

	// Set up ack channel from scheduler to vertex
	ackChan := make(chan vertex.Request, 1024)
	v.SetInAckChan(ackChan)

	// Insert the vertex into scheduler
	s.graph.InsertVertex(v)
}

func (s *SimpleScheduler) BuildEdge(
	src vertex.Vertex,
	target vertex.Vertex,
	dir constants.VertexInDir,
) (vertex.Edge, error) {
	if src == nil {
		return nil, errors.New("src vertex cannot be nil")
	}
	if target == nil {
		return nil, errors.New("target vertex connot be nil")
	}

	e := vertex.NewEdge(src.GetId(), target.GetId())
	s.graph.InsertEdge(e, dir)

	return e, nil
}

// TODO Step is for finer control of scheduler.
func (s *SimpleScheduler) Step() error {
	return nil
}

// Run starts the scheduler after all vertices are registered.
// This also pre-processes and optimizes the graph.
func (s *SimpleScheduler) Run() error {
	defer s.cancelFunc()

	s.graph.PreProcess()

	var wg sync.WaitGroup
	for id := range s.graph.VertexMap {
		wg.Add(1)
		v := s.graph.VertexMap[id].vertex
		go v.Start(wg)
	}

	go s.Serve()
	wg.Wait()

	return nil
}

func (s *SimpleScheduler) Serve() {
	for {
		select {
		case <-s.ctx.Done():
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
	src := e.GetSrc()
	target := e.GetTarget()
	node, exist := s.graph.VertexMap[target]
	if !exist {
		return errors.New(fmt.Sprintf("vertex not found in graph when doing SendBy with id: %d", target))
	}
	dir, err := s.graph.GetDir(src, target)
	if err != nil {
		return err
	}
	ch := node.vertex.GetInTaskChans()[dir]
	if ch == nil {
		return errors.New("target vertex is not set up with request channel when doing SendBy")
	}
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
	ch := node.vertex.GetInTaskChans()[constants.VertexInDir_Default]
	if ch == nil {
		return errors.New("target vertex is not set up with request channel when doing NotifyAt")
	}
	ch <- vertex.Request{
		Typ:  constants.RequestType_OnNotify,
		Edge: e,
		Ts:   req.Ts,
		Msg:  req.Msg,
	}
	return nil
}
