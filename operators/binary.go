package operators

import (
	"errors"
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

type BinaryHandle interface {
	handles.VertexHandle
}

type BinaryHandleCore struct {
	handles.VertexHandle
}

type BinaryOp interface {
	scope.Scope
	GenericBinaryOp
}

type BinaryOpCore struct {
	*OpCore
	handle1 BinaryHandle
	handle2 BinaryHandle

	f1 DataCallback
	f2 DataCallback
}

func (op *BinaryOpCore) Start(wg sync.WaitGroup) error {
	defer wg.Done()
	for {
		select {
		case <-op.Done():
			return nil
		case req := <-op.handle1.MsgRecv():
			if err := op.handleReq(&req, constants.BinaryType_Left); err != nil {
				utils.Logger().Error(err.Error())
			}
		case req := <-op.handle2.MsgRecv():
			if err := op.handleReq(&req, constants.BinaryType_Right); err != nil {
				utils.Logger().Error(err.Error())
			}
		}
	}
}

func (op *BinaryOpCore) handleReq(req *request.Request, bt constants.BinaryType) error {
	typ := req.Typ
	edge := req.Edge
	msg := req.Msg
	ts := req.Ts
	if !(timestamp.LE(&op.currTs, &ts)) {
		return errors.New("cannot accept an earlier timestamp from binary operator")
	}
	op.currTs = ts

	if typ == constants.RequestType_OnRecv {
		if bt == constants.BinaryType_Left {
			return op.OnRecv1(edge, msg, ts)
		} else if bt == constants.BinaryType_Right {
			return op.OnRecv2(edge, msg, ts)
		} else {
			return fmt.Errorf("invalid binary type with value: %d", bt)
		}
	} else if typ == constants.RequestType_OnNotify {
		if bt == constants.BinaryType_Left {
			return op.OnNotify1(ts)
		} else if bt == constants.BinaryType_Right {
			return op.OnNotify2(ts)
		} else {
			return fmt.Errorf("invalid binary type with value: %d", bt)
		}
	} else {
		return fmt.Errorf("invalid request type with value: %d", typ)
	}
}

func (op *BinaryOpCore) OnRecv1(e edge.Edge, msg request.Message, ts timestamp.Timestamp) error {
	if err := op.coreDecreOC(e, ts, op.handle1); err != nil {
		return err
	}

	m, err := op.f1(e, msg, ts)
	if err != nil {
		return err
	}

	// Send the result message to next target to continue the dataflow
	if err := op.SendBy(edge.NewEdge(op.id, op.target), m, ts); err != nil {
		return err
	}
	return nil
}

func (op *BinaryOpCore) OnRecv2(e edge.Edge, msg request.Message, ts timestamp.Timestamp) error {
	if err := op.coreDecreOC(e, ts, op.handle2); err != nil {
		return err
	}

	m, err := op.f2(e, msg, ts)
	if err != nil {
		return err
	}

	// Send the result message to next target to continue the dataflow
	if err := op.SendBy(edge.NewEdge(op.id, op.target), m, ts); err != nil {
		return err
	}
	return nil
}

func (op *BinaryOpCore) OnNotify1(ts timestamp.Timestamp) error {
	return nil
}

func (op *BinaryOpCore) OnNotify2(ts timestamp.Timestamp) error {
	return nil
}

func (op *BinaryOpCore) SendBy(e edge.Edge, msg request.Message, ts timestamp.Timestamp) error {
	return op.coreSendBy(e, msg, ts, op.handle1)
}

func (op *BinaryOpCore) NotifyAt(ts timestamp.Timestamp) error {
	return nil
}
