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

	ch := make(chan request.InputDatum)

	f := func(w worker.Worker) error {
		w.Dataflow(func(s scope.Scope) error {
			input := operators.NewInput(s, ch)
			input.Loop(
				func(ups operators.Operator) operators.Operator {
					return ups.
						Inspect(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
							println(fmt.Sprintf("inspect operatro 1 got ts: %s", ts.ToString()))
							println(fmt.Sprintf("inspect operator 1 inside loop received message: %s", msg.ToString()))
							return iterator.IterFromSingleton(msg), nil
						}).
						Inspect(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
							println(fmt.Sprintf("inspect operatro 2 got ts: %s", ts.ToString()))
							println(fmt.Sprintf("inspect operator 2 inside loop received message: %s", msg.ToString()))
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
					println(fmt.Sprintf("inspect operator 3 outside loop received message: %s", msg.ToString()))
					println(fmt.Sprintf("inspect operatro 3 got ts: %s", ts.ToString()))
					return nil, nil
				})
			return nil
		})
		return nil
	}
	go step.Start(f)

	for i := 0; i < 1; i++ {
		ch <- request.NewInputRaw(
			request.NewMessage([]byte(strconv.Itoa(i))),
			*timestamp.NewTimestamp(),
		)
	}

	time.Sleep(time.Second * 5)
}
