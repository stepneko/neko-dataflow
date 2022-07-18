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

type IngressHandle interface {
	handles.VertexHandle
}

type IngressHandleCore struct {
	handles.VertexHandle
}

type IngressOp interface {
	scope.Scope
	Operator
	DoubleInput
}

type IngressOpCore struct {
	*OpCore
	handle1 IngressHandle
	handle2 IngressHandle
}

func (op *IngressOpCore) Start(wg sync.WaitGroup) error {
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

func (op *IngressOpCore) handleReq(req *request.Request, bt constants.BinaryType) error {
	typ := req.Typ
	edge := req.Edge
	msg := req.Msg
	ts := req.Ts

	if err := op.tsCheckAndUpdate(&ts); err != nil {
		return err
	}

	if typ == constants.RequestType_OnRecv {
		if bt == constants.BinaryType_Left {
			return op.OnRecv1(edge, &msg, ts)
		} else if bt == constants.BinaryType_Right {
			return op.OnRecv2(edge, &msg, ts)
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

func (op *IngressOpCore) OnRecv1(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error {
	if err := op.coreDecreOC(e, ts, op.handle1); err != nil {
		return err
	}

	newTs := timestamp.CopyTimestampFrom(&ts)
	if err := timestamp.HandleTimestamp(constants.VertexType_Ingress, newTs); err != nil {
		return err
	}

	if err := op.SendBy(edge.NewEdge(op.id, op.target), msg, *newTs); err != nil {
		return err
	}
	return nil
}

func (op *IngressOpCore) OnRecv2(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error {
	// Use handle1 because worker acks back to IngressOpCore vertex on IncreOC and DecreOC
	// using handle1, no matter if the original message comes from OnRecv1() or OnRecv2()
	// This is because the ack is only for unblocking current computation so just
	// make OnRecv1() and OnRecv2() share the same ack handle and this is sufficient.
	//
	// Actually from the worker side, the worker picks vertex handle by map[src, src] or
	// map[target, target], rather than map[src, target], therefore only handle1 will be picked.
	if err := op.coreDecreOC(e, ts, op.handle1); err != nil {
		return err
	}

	if err := op.SendBy(edge.NewEdge(op.id, op.target), msg, ts); err != nil {
		return err
	}
	return nil
}

func (op *IngressOpCore) OnNotify1(ts timestamp.Timestamp) error {
	return nil
}

func (op *IngressOpCore) OnNotify2(ts timestamp.Timestamp) error {
	return nil
}

func (op *IngressOpCore) SendBy(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error {
	return op.coreSendBy(e, msg, ts, op.handle1)
}

func (op *IngressOpCore) NotifyAt(ts timestamp.Timestamp) error {
	return nil
}
