package operators

import (
	"errors"
	"sync"

	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/handles"
	"github.com/stepneko/neko-dataflow/request"
	"github.com/stepneko/neko-dataflow/scope"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/utils"
)

type InputHandle interface {
	handles.VertexHandle
}

type InputHandleCore struct {
	handles.SimpleWorkerHandle
}

type InputOp interface {
	scope.Scope
	GenericUnaryOp
}

type InputOpCore struct {
	*OpCore
	handle  InputHandle
	inputCh chan request.InputRaw
}

func (op *InputOpCore) Start(wg sync.WaitGroup) error {
	defer wg.Done()
	for {
		select {
		case <-op.Done():
			return nil
		case req := <-op.handle.MsgRecv():
			if err := op.handleReq(&req); err != nil {
				utils.Logger().Error(err.Error())
			}
		case inData := <-op.inputCh:
			if err := op.handleInput(&inData); err != nil {
				utils.Logger().Error(err.Error())
			}
		}
	}
}

func (op *InputOpCore) handleReq(req *request.Request) error {
	// So far nothing to do. This is just a place holder
	// because NotifyAt may need to be handled here
	return nil
}

func (op *InputOpCore) handleInput(inData *request.InputRaw) error {
	msg := inData.Msg
	ts := inData.Ts
	if !(timestamp.LE(&op.currTs, &ts)) {
		return errors.New("cannot accept an earlier timestamp from input operator")
	}
	op.currTs = ts

	e := edge.NewEdge(op.id, op.target)
	if err := op.SendBy(e, msg, ts); err != nil {
		return err
	}
	return nil
}

func (op *BinaryOpCore) OnRecv(e edge.Edge, msg request.Message, ts timestamp.Timestamp) error {
	return nil
}

func (op *InputOpCore) OnNotify(ts timestamp.Timestamp) error {
	return nil
}

func (op *InputOpCore) SendBy(e edge.Edge, msg request.Message, ts timestamp.Timestamp) error {
	return op.coreSendBy(e, msg, ts, op.handle)
}

func (op *InputOpCore) NotifyAt(ts timestamp.Timestamp) error {
	return nil
}
