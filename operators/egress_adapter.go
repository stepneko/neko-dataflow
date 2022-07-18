package operators

import (
	"fmt"
	"sync"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/handles"
	"github.com/stepneko/neko-dataflow/request"
	"github.com/stepneko/neko-dataflow/scope"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/utils"
)

type EgressAdapterHandle interface {
	handles.VertexHandle
}

type EgressAdapterHandleCore struct {
	handles.VertexHandle
}

type EgressAdapterOp interface {
	scope.Scope
	Operator
	SingleInput
	DoubleOutput
}

type EgressAdapterOpCore struct {
	*OpCore
	handle  InputHandle
	target2 constants.VertexId
	f       FilterCallback
}

func (op *EgressAdapterOpCore) Start(wg sync.WaitGroup) error {
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

func (op *EgressAdapterOpCore) handleReq(req *request.Request) error {
	typ := req.Typ
	edge := req.Edge
	msg := req.Msg
	ts := req.Ts

	if err := op.tsCheckAndUpdate(&ts); err != nil {
		return err
	}

	if typ == constants.RequestType_OnRecv {
		return op.OnRecv(edge, &msg, ts)
	} else if typ == constants.RequestType_OnNotify {
		return op.OnNotify(ts)
	} else {
		return fmt.Errorf("invalid request type with value: %d", typ)
	}
}

func (op *EgressAdapterOpCore) OnRecv(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error {
	if err := op.coreDecreOC(e, ts, op.handle); err != nil {
		return err
	}

	flag, err := op.f(e, msg, ts)
	if err != nil {
		return err
	}

	// If loop boolean flag is true, the dataflow should move back through the loop,
	// via target2 which is the feedback operator
	if flag {
		if err := op.SendBy(edge.NewEdge(op.id, op.target2), msg, ts); err != nil {
			return err
		}
		return nil
		// Otherwise dataflow should move via target which is the EgressAdapter operator
	} else {
		if err := op.SendBy(edge.NewEdge(op.id, op.target), msg, ts); err != nil {
			return err
		}
		return nil
	}
}

func (op *EgressAdapterOpCore) OnNotify(ts timestamp.Timestamp) error {
	return nil
}

func (op *EgressAdapterOpCore) SendBy(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error {
	return op.coreSendBy(e, msg, ts, op.handle)
}

func (op *EgressAdapterOpCore) NotifyAt(ts timestamp.Timestamp) error {
	return nil
}

func (op *EgressAdapterOpCore) SetTarget2(vid constants.VertexId) {
	op.target2 = vid
}
