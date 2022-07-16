package step

import (
	"context"

	"github.com/stepneko/neko-dataflow/worker"
)

type StartFn = func(w worker.Worker) error

func Start(fn StartFn) error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	w := worker.NewSimpleWorker(ctx)
	fn(w)
	if err := w.Run(); err != nil {
		return err
	}
	return nil
}
