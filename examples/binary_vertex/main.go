package main

import (
	"strconv"
	"time"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/scheduler"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/utils"
	"github.com/stepneko/neko-dataflow/vertex"
)

func main() {
	s := scheduler.NewScheduler()

	input1 := vertex.NewInputVertex()
	s.RegisterVertex(input1)

	input2 := vertex.NewInputVertex()
	s.RegisterVertex(input2)

	v := vertex.NewBinaryVertex()
	s.RegisterVertex(v)

	e1, err := s.BuildEdge(input1, v, constants.VertexInDir_Left)
	if err != nil {
		utils.Logger().Error(err.Error())
		return
	}

	e2, err := s.BuildEdge(input2, v, constants.VertexInDir_Right)
	if err != nil {
		utils.Logger().Error(err.Error())
		return
	}

	input1.OnRecv(func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		println("input1 on recv: " + m.ToString())
		input1.SendBy(e1, m, ts)
		return nil
	})
	input2.OnRecv(func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		println("input2 on recv: " + m.ToString())
		input2.SendBy(e2, m, ts)
		return nil
	})
	v.OnRecvLeft(func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		println("v on recv from left: " + m.ToString())
		return nil
	})
	v.OnRecvRight(func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		println("v on recv from right: " + m.ToString())
		return nil
	})

	go s.Run()

	println("starting here")

	ts := timestamp.NewTimestamp()
	for i := 0; i < 5; i++ {
		m := vertex.NewMessage([]byte(strconv.Itoa(i)))
		input1.Send(*ts, *m)
	}
	for i := 10; i < 15; i++ {
		m := vertex.NewMessage([]byte(strconv.Itoa(i)))
		input2.Send(*ts, *m)
	}
	time.Sleep(time.Hour)
}
