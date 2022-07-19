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
)

type FilterHandle interface {
	handles.VertexHandle
}

type FilterHandleCore struct {
	handles.SimpleWorkerHandle
}

type FilterOp interface {
	scope.Scope
	Operator
	SingleInput
}

type FilterOpCore struct {
	*OpCore
	handle InputHandle
	f      FilterCallback
}

func (op *FilterOpCore) Start(wg sync.WaitGroup) error {
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

func (op *FilterOpCore) handleReq(req *request.Request) error {
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

func (op *FilterOpCore) OnRecv(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error {
	if err := op.coreDecreOC(e, ts, op.handle); err != nil {
		return err
	}

	flag, err := op.f(e, msg, ts)
	if err != nil {
		return err
	}

	if !flag {
		return nil
	}

	if err := op.SendBy(edge.NewEdge(op.id, op.target), msg, ts); err != nil {
		return err
	}
	return nil
}

func (op *FilterOpCore) OnNotify(ts timestamp.Timestamp) error {
	return nil
}

func (op *FilterOpCore) SendBy(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error {
	return op.coreSendBy(e, msg, ts, op.handle)
}

func (op *FilterOpCore) NotifyAt(ts timestamp.Timestamp) error {
	return nil
}
