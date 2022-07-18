package tests

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/iterator"
	"github.com/stepneko/neko-dataflow/operators"
	"github.com/stepneko/neko-dataflow/request"
	"github.com/stepneko/neko-dataflow/scope"
	"github.com/stepneko/neko-dataflow/step"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/worker"
	"github.com/stretchr/testify/assert"
)

func TestLoopCase(t *testing.T) {

	ch := make(chan request.InputDatum, 1024)
	inspectCh1 := make(chan string, 1024)
	inspectCh2 := make(chan string, 1024)
	inspectCh3 := make(chan string, 1024)

	f := func(w worker.Worker) error {
		w.Dataflow(func(s scope.Scope) error {
			input := operators.NewInput(s, ch)
			input.Loop(
				func(ups operators.Operator) operators.Operator {
					return ups.
						Inspect(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
							inspectCh1 <- fmt.Sprintf("inspect operator 1 inside loop received message: %s", msg.ToString())
							return iterator.IterFromSingleton(msg), nil
						}).
						Inspect(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
							inspectCh2 <- fmt.Sprintf("inspect operator 2 inside loop received message: %s", msg.ToString())
							val, err := strconv.Atoi(msg.ToString())
							if err != nil {
								return nil, err
							}
							val += 1
							newMsg := request.NewMessage([]byte(strconv.Itoa(val)))
							return iterator.IterFromSingleton(newMsg), nil
						})
				},
				func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (bool, error) {
					val, err := strconv.Atoi(msg.ToString())
					if err != nil {
						return false, err
					}
					if val < 5 {
						return true, nil
					}
					return false, nil
				},
			).
				Inspect(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
					inspectCh3 <- fmt.Sprintf("inspect operator 3 outside loop received message: %s", msg.ToString())
					return nil, nil
				})
			return nil
		})
		return nil
	}
	go step.Start(f)

	i := 0
	ch <- request.NewInputRaw(
		request.NewMessage([]byte(strconv.Itoa(i))),
		*timestamp.NewTimestamp(),
	)

	for i := 0; i < 5; i++ {
		s1 := <-inspectCh1
		assert.Equal(t, s1, fmt.Sprintf("inspect operator 1 inside loop received message: %d", i))
		s2 := <-inspectCh2
		assert.Equal(t, s2, fmt.Sprintf("inspect operator 2 inside loop received message: %d", i))
	}
	s3 := <-inspectCh3
	assert.Equal(t, s3, fmt.Sprintf("inspect operator 3 outside loop received message: %d", 5))
}
