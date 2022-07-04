package main

import (
	"strconv"
	"time"

	"github.com/stepneko/neko-dataflow/scheduler"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/vertex"
)

// Build a simple scheduler with a simple input vertex.
// Basic communication between scheduler and veretx,
// and how data is being processed.
func main() {
	// Initialize the scheduler.
	s := scheduler.NewScheduler()

	// Set up vertices and edges.
	// Create input vertex.
	input := vertex.NewInputVertex()
	// Register it to the scheduler.
	s.RegisterVertex(input)

	// Create a generic vertex to connect with input.
	v1 := vertex.NewGenericVertex()
	// Register it to the scheduler.
	s.RegisterVertex(v1)

	// Create an edge between them.
	e1, err := s.BuildEdge(input, v1)
	if err != nil {
		println(err.Error())
		return
	}

	// Create another generic vertex to connect with v1.
	v2 := vertex.NewGenericVertex()
	// Register it to the scheduler
	s.RegisterVertex(v2)

	e2, err := s.BuildEdge(v1, v2)
	if err != nil {
		println(err.Error())
		return
	}

	// Define behaviors of input vertex.
	input.On(vertex.CallbackType_OnRecv, func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		println("input on recv: " + m.ToString())
		input.SendBy(e1, m, ts)
		return nil
	})
	input.On(vertex.CallbackType_OnNotify, func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		println("input on notify")
		return nil
	})

	v1.On(vertex.CallbackType_OnRecv, func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		println("v1 on recv: " + m.ToString())
		v1.SendBy(e2, m, ts)
		return nil
	})
	v1.On(vertex.CallbackType_OnNotify, func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		println("v1 on notify")
		return nil
	})
	v2.On(vertex.CallbackType_OnRecv, func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		println("v2 on recv: " + m.ToString())
		return nil
	})
	v2.On(vertex.CallbackType_OnNotify, func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		println("v2 on notify")
		return nil
	})

	// Start the scheduler
	go s.Run()

	// Start streaming data into the system
	ts := timestamp.NewTimestamp()
	for i := 0; i < 5; i++ {
		m := vertex.NewMessage([]byte(strconv.Itoa(i)))
		input.Send(*ts, *m)
	}
	input.Notify(*ts)
	v1.NotifyAt(*ts)
	v2.NotifyAt(*ts)
	time.Sleep(time.Hour)
}
