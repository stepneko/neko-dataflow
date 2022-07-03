package main

import (
	"github.com/stepneko/neko-dataflow/scheduler"
)

func main() {
	s := scheduler.NewScheduler()
	s.Step()
}
