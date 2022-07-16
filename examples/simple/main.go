package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/stepneko/neko-dataflow/edge"
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
				Inspect(func(e edge.Edge, msg request.Message, ts timestamp.Timestamp) (request.Message, error) {
					println(fmt.Sprintf("inspect operator 1 received message: %s", msg.ToString()))
					return *request.NewMessage([]byte(msg.ToString())), nil
				}).
				Inspect(func(e edge.Edge, msg request.Message, ts timestamp.Timestamp) (request.Message, error) {
					println(fmt.Sprintf("inspect operator 2 received message: %s", msg.ToString()))
					return *request.NewMessage([]byte(msg.ToString())), nil
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

	time.Sleep(time.Hour)
}
