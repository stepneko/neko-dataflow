package operators

import (
	"fmt"
	"sync"

	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/handles"
	"github.com/stepneko/neko-dataflow/request"
	"github.com/stepneko/neko-dataflow/scope"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/utils"
	"github.com/stepneko/neko-dataflow/vertex"
)

type FeedbackHandle interface {
	handles.VertexHandle
}

type FeedbackHandleCore struct {
	handles.SimpleWorkerHandle
}

type FeedbackOp interface {
	scope.Scope
	Operator
	SingleInput
}

type FeedbackOpCore struct {
	*OpCore
	handle InputHandle
}

func (op *FeedbackOpCore) Start(wg sync.WaitGroup) error {
	defer wg.Done()
	for {
		select {
		case <-op.Done():
			return nil
		case req := <-op.handle.MsgRecv():
			if err := op.handleReq(&req); err != nil {
				utils.Logger().Error(err.Error())
			}
		}
	}
}

func (op *FeedbackOpCore) handleReq(req *request.Request) error {
	typ := req.Type
	edge := req.Edge
	msg := req.Msg
	ts := req.Ts

	if err := op.tsCheckAndUpdate(&ts); err != nil {
		return err
	}

	if typ == request.Type_OnRecv {
		return op.OnRecv(edge, &msg, ts)
	} else if typ == request.Type_OnNotify {
		return op.OnNotify(ts)
	} else {
		return fmt.Errorf("invalid request type with value: %d", typ)
	}
}

func (op *FeedbackOpCore) OnRecv(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error {
	if err := op.coreDecreOC(e, ts, op.handle); err != nil {
		return err
	}

	newTs := timestamp.CopyTimestampFrom(&ts)
	if err := timestamp.HandleTimestamp(vertex.Type_Feedback, newTs); err != nil {
		return err
	}

	if err := op.SendBy(edge.NewEdge(op.id, op.target), msg, *newTs); err != nil {
		return err
	}
	return nil
}

func (op *FeedbackOpCore) OnNotify(ts timestamp.Timestamp) error {
	return nil
}

func (op *FeedbackOpCore) SendBy(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error {
	return op.coreSendBy(e, msg, ts, op.handle)
}

func (op *FeedbackOpCore) NotifyAt(ts timestamp.Timestamp) error {
	return nil
}
