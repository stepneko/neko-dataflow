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
				Inspect(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
					arr := []*request.Message{}
					val, err := strconv.Atoi(msg.ToString())
					if err != nil {
						return nil, err
					}
					println(fmt.Sprintf("inspect operator 1 received message: %d", val))
					for i := 0; i < 5; i++ {
						arr = append(arr, request.NewMessage([]byte(strconv.Itoa(val*5+i))))
					}
					return iterator.IterFromArray(arr), nil
				}).
				Inspect(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
					arr := []*request.Message{}
					val, err := strconv.Atoi(msg.ToString())
					if err != nil {
						return nil, err
					}
					println(fmt.Sprintf("inspect operator 2 received message: %d", val))
					for i := 0; i < 5; i++ {
						arr = append(arr, request.NewMessage([]byte(strconv.Itoa(val*5+i))))
					}
					return iterator.IterFromArray(arr), nil
				}).
				Inspect(func(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) (iterator.Iterator[*request.Message], error) {
					val, err := strconv.Atoi(msg.ToString())
					if err != nil {
						return nil, err
					}
					println(fmt.Sprintf("inspect operator 3 received message: %d", val))

					return nil, nil
				})
			return nil
		})
		return nil
	}

	go step.Start(f)

	for i := 0; i < 5; i++ {
		ch <- request.InputRaw{
			Msg: *request.NewMessage([]byte(strconv.Itoa(i))),
			Ts:  *timestamp.NewTimestamp(),
		}
	}

	time.Sleep(time.Second * 5)
}
