package worker

import (
	"context"
	"strconv"
	"testing"

	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/iterator"
	"github.com/stepneko/neko-dataflow/operators"
	"github.com/stepneko/neko-dataflow/request"
	"github.com/stepneko/neko-dataflow/scope"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stretchr/testify/assert"
)

func TestSimpleWorker(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	w := NewSimpleWorker(ctx)

	assert.Equal(t, w, w.ToScope())

	inputCh := make(chan request.InputDatum, 1024)
	inspectMsgCh := make(chan string, 1024)
	inspectTsCh := make(chan int, 1024)
	w.Dataflow(func(s scope.Scope) error {
		operators.
			NewInput(s, inputCh).
			Inspect(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
				inspectMsgCh <- msg.ToString()
				inspectTsCh <- ts.Counters[0]
				return nil, nil
			})
		return nil
	})

	go w.Run()

	for i := 0; i < 5; i++ {
		inputCh <- request.NewInputRaw(
			request.NewMessage([]byte(strconv.Itoa(i))),
			*timestamp.NewTimestampWithParams(0, []int{i}),
		)
	}

	for i := 0; i < 5; i++ {
		msg := <-inspectMsgCh
		assert.Equal(t, msg, strconv.Itoa(i))
		tsCounter := <-inspectTsCh
		assert.Equal(t, tsCounter, i)
	}
}
