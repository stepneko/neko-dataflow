package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/iterator"
	"github.com/stepneko/neko-dataflow/operators"
	"github.com/stepneko/neko-dataflow/request"
	"github.com/stepneko/neko-dataflow/scope"
	"github.com/stepneko/neko-dataflow/step"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/worker"
)

func main() {

	ch1 := make(chan request.InputRaw, 1024)
	ch2 := make(chan request.InputRaw, 1024)

	f := func(w worker.Worker) error {
		w.Dataflow(func(s scope.Scope) error {
			input1 := operators.NewInput(s, ch1)
			input2 := operators.NewInput(s, ch2)

			input1.
				Binary(
					input2,
					func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
						println(fmt.Sprintf("binary operator received message from input 1: %s", msg.ToString()))
						return iterator.IterFromSingleton(msg), nil
					},
					func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
						println(fmt.Sprintf("binary operator received message from input 2: %s", msg.ToString()))
						return iterator.IterFromSingleton(msg), nil
					},
				).
				Inspect(
					func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
						println(fmt.Sprintf("inspect operator received message: %s", msg.ToString()))
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
	}

	for i := 10; i < 15; i++ {
		ch2 <- request.InputRaw{
			Msg: *request.NewMessage([]byte(strconv.Itoa(i))),
			Ts:  *timestamp.NewTimestamp(),
		}
	}

	time.Sleep(time.Second * 5)
}
