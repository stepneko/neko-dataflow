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

func TestBinaryCase(t *testing.T) {

	ch1 := make(chan request.InputRaw, 1024)
	ch2 := make(chan request.InputRaw, 1024)

	binaryCh1 := make(chan string, 1024)
	binaryCh2 := make(chan string, 1024)

	inspectCh1 := make(chan string, 1024)
	inspectCh2 := make(chan string, 1024)

	f := func(w worker.Worker) error {
		w.Dataflow(func(s scope.Scope) error {
			input1 := operators.NewInput(s, ch1)
			input2 := operators.NewInput(s, ch2)

			input1.
				Binary(
					input2,
					func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
						binaryCh1 <- fmt.Sprintf("binary operator received message from input 1: %s", msg.ToString())
						return iterator.IterFromSingleton(msg), nil
					},
					func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
						binaryCh2 <- fmt.Sprintf("binary operator received message from input 2: %s", msg.ToString())
						return iterator.IterFromSingleton(msg), nil
					},
				).
				Inspect(
					func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
						str := msg.ToString()
						val, err := strconv.Atoi(str)
						if err != nil {
							return iterator.IterFromSingleton(request.NewMessage([]byte{})), err
						}
						if val < 10 {
							inspectCh1 <- fmt.Sprintf("inspect operator received message: %s", str)
						} else {
							inspectCh2 <- fmt.Sprintf("inspect operator received message: %s", str)
						}

						return iterator.IterFromSingleton(msg), nil
					},
				)
			return nil
		})
		return nil
	}
	go step.Start(f)

	for i := 0; i < 5; i++ {
		ch1 <- request.InputRaw{
			Msg: *request.NewMessage([]byte(strconv.Itoa(i))),
			Ts:  *timestamp.NewTimestamp(),
		}
		ch2 <- request.InputRaw{
			Msg: *request.NewMessage([]byte(strconv.Itoa(i + 10))),
			Ts:  *timestamp.NewTimestamp(),
		}
	}

	for i := 0; i < 5; i++ {
		binaryS1 := <-binaryCh1
		assert.Equal(t, binaryS1, fmt.Sprintf("binary operator received message from input 1: %d", i))
		binaryS2 := <-binaryCh2
		assert.Equal(t, binaryS2, fmt.Sprintf("binary operator received message from input 2: %d", i+10))
		inspectS1 := <-inspectCh1
		assert.Equal(t, inspectS1, fmt.Sprintf("inspect operator received message: %d", i))
		inspectS2 := <-inspectCh2
		assert.Equal(t, inspectS2, fmt.Sprintf("inspect operator received message: %d", i+10))
	}

}
