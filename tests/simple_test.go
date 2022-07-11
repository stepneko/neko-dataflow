package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/scheduler"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/utils"
	"github.com/stepneko/neko-dataflow/vertex"
)

func TestSimpleCase(t *testing.T) {
	chRecvInput := make(chan vertex.Message, 1024)
	chRecvV1 := make(chan vertex.Message, 1024)
	chRecvV2 := make(chan vertex.Message, 1024)
	chNotifyInput := make(chan vertex.Message, 1024)
	chNotifyV1 := make(chan vertex.Message, 1024)
	chNotifyV2 := make(chan vertex.Message, 1024)

	s := scheduler.NewScheduler()

	input := vertex.NewInputVertex()
	s.RegisterVertex(input)

	v1 := vertex.NewUnaryVertex()
	s.RegisterVertex(v1)

	e1, err := s.BuildEdge(input, v1, constants.VertexInDir_Default)
	if err != nil {
		utils.Logger().Error(err.Error())
		return
	}

	v2 := vertex.NewUnaryVertex()
	s.RegisterVertex(v2)

	e2, err := s.BuildEdge(v1, v2, constants.VertexInDir_Default)
	if err != nil {
		utils.Logger().Error(err.Error())
		return
	}

	input.OnRecv(func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		chRecvInput <- m
		input.SendBy(e1, m, ts)
		return nil
	})
	input.OnNotify(func(ts timestamp.Timestamp) error {
		chNotifyInput <- *vertex.NewMessage([]byte("input notified"))
		return nil
	})

	v1.OnRecv(func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		chRecvV1 <- m
		v1.SendBy(e2, m, ts)
		return nil
	})
	v1.OnNotify(func(ts timestamp.Timestamp) error {
		chNotifyV1 <- *vertex.NewMessage([]byte("v1 notified"))
		return nil
	})

	v2.OnRecv(func(e vertex.Edge, m vertex.Message, ts timestamp.Timestamp) error {
		chRecvV2 <- m
		return nil
	})
	v2.OnNotify(func(ts timestamp.Timestamp) error {
		chNotifyV2 <- *vertex.NewMessage([]byte("v2 notified"))
		return nil
	})

	go s.Run()

	ts := timestamp.NewTimestamp()
	for i := 0; i < 5; i++ {
		m := vertex.NewMessage([]byte(strconv.Itoa(i)))
		input.Send(*ts, *m)
	}
	input.Notify(*ts)
	v1.NotifyAt(*ts)
	v2.NotifyAt(*ts)

	for i := 0; i < 5; i++ {
		mRecvInput := <-chRecvInput
		assert.Equal(t, mRecvInput.ToString(), strconv.Itoa(i))
		mRecvV1 := <-chRecvV1
		assert.Equal(t, mRecvV1.ToString(), strconv.Itoa(i))
		mRecvV2 := <-chRecvV2
		assert.Equal(t, mRecvV2.ToString(), strconv.Itoa(i))
	}

	mNotifyInput := <-chNotifyInput
	assert.Equal(t, mNotifyInput.ToString(), "input notified")
	mNotifyV1 := <-chNotifyV1
	assert.Equal(t, mNotifyV1.ToString(), "v1 notified")
	mNotifyV2 := <-chNotifyV2
	assert.Equal(t, mNotifyV2.ToString(), "v2 notified")
}
