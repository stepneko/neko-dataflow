package scheduler

import "github.com/stepneko/neko-dataflow/vertex"

type VertexStatus struct {
	taskChan chan vertex.Request
	ackChan  chan vertex.Request
}

func NewVertexStatus(
	taskChan chan vertex.Request,
	ackChan chan vertex.Request,
) *VertexStatus {
	return &VertexStatus{
		taskChan: taskChan,
		ackChan:  ackChan,
	}
}
