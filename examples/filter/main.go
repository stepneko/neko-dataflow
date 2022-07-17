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
	ch := make(chan request.InputRaw, 1024)

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
						return true, nil
					} else {
						return false, nil
					}
				}).
				Inspect(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
					println(fmt.Sprintf("inspector operator received message: %s", msg.ToString()))
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

	time.Sleep(time.Second * 5)
}
