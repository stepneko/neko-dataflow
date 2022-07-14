package tests

import (
	"strconv"
	"testing"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/scheduler"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/utils"
	"github.com/stepneko/neko-dataflow/vertex"
	"github.com/stretchr/testify/assert"
)

func TestBinaryCase(t *testing.T) {
	chRecvInput1 := make(chan vertex.Message, 1024)
	chRecvInput2 := make(chan vertex.Message, 1024)
	chRecvVLeft := make(chan vertex.Message, 1024)
	chRecvVRight := make(chan vertex.Message, 1024)

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
		chRecvInput1 <- m
		input1.SendBy(e1, m, ts)
		return nil
	})
	input2.OnRecv(func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		chRecvInput2 <- m
		input2.SendBy(e2, m, ts)
		return nil
	})
	v.OnRecvLeft(func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		chRecvVLeft <- m
		return nil
	})
	v.OnRecvRight(func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		chRecvVRight <- m
		return nil
	})

	go s.Run()

	ts := timestamp.NewTimestamp()
	for i := 0; i < 5; i++ {
		m := vertex.NewMessage([]byte(strconv.Itoa(i)))
		input1.Send(*ts, *m)
	}

	for i := 10; i < 15; i++ {
		m := vertex.NewMessage([]byte(strconv.Itoa(i)))
		input2.Send(*ts, *m)
	}

	for i := 0; i < 5; i++ {
		mRecvInput1 := <-chRecvInput1
		assert.Equal(t, mRecvInput1.ToString(), strconv.Itoa(i))
		mRecvVLeft := <-chRecvVLeft
		assert.Equal(t, mRecvVLeft.ToString(), strconv.Itoa(i))

	}

	for i := 10; i < 15; i++ {
		mRecvInput2 := <-chRecvInput2
		assert.Equal(t, mRecvInput2.ToString(), strconv.Itoa(i))
		mRecvVRight := <-chRecvVRight
		assert.Equal(t, mRecvVRight.ToString(), strconv.Itoa(i))
	}
}
