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

func TestFilterCase(t *testing.T) {
	ch := make(chan request.InputRaw, 1024)
	filterCh1 := make(chan string, 1024)
	filterCh2 := make(chan string, 1024)
	inspectCh := make(chan string, 1024)

	f := func(w worker.Worker) error {
		w.Dataflow(func(s scope.Scope) error {
			operators.
				NewInput(s, ch).
				Filter(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (bool, error) {
					val, err := strconv.Atoi(msg.ToString())
					if err != nil {
						return false, err
					}
					if val%2 == 0 {
						filterCh1 <- fmt.Sprintf("even numbers going through filter operator: %d", val)
						return true, nil
					} else {
						filterCh2 <- fmt.Sprintf("odd numbers blocked by filter operator: %d", val)
						return false, nil
					}
				}).
				Inspect(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
					inspectCh <- fmt.Sprintf("inspector operator received message: %s", msg.ToString())
					return iterator.IterFromSingleton(request.NewMessage([]byte{})), nil
				})
			return nil
		})
		return nil
	}

	go step.Start(f)

	for i := 0; i < 10; i++ {
		ch <- request.InputRaw{
			Msg: *request.NewMessage([]byte(strconv.Itoa(i))),
			Ts:  *timestamp.NewTimestamp(),
		}
	}

	for i := 0; i < 5; i++ {
		filterS1 := <-filterCh1
		assert.Equal(t, filterS1, fmt.Sprintf("even numbers going through filter operator: %d", i*2))
		filterS2 := <-filterCh2
		assert.Equal(t, filterS2, fmt.Sprintf("odd numbers blocked by filter operator: %d", i*2+1))
		inspectS := <-inspectCh
		assert.Equal(t, inspectS, fmt.Sprintf("inspector operator received message: %d", i*2))
	}
}
