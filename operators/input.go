package operators

import (
	"sync"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/handles"
	"github.com/stepneko/neko-dataflow/request"
	"github.com/stepneko/neko-dataflow/scope"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/utils"
	"github.com/stepneko/neko-dataflow/vertex"
)

type InputHandle interface {
	handles.VertexHandle
}

type InputHandleCore struct {
	handles.SimpleWorkerHandle
}

type InputOp interface {
	scope.Scope
	Operator
	SingleInput
}

type InputOpCore struct {
	*OpCore
	handle  InputHandle
	inputCh chan request.InputDatum
}

// NewInput creates input operator from scope
func NewInput(s scope.Scope, inputCh chan request.InputDatum) InputOp {
	taskCh := make(chan request.Request, constants.ChanCacapity)
	ackCh := make(chan request.Request, constants.ChanCacapity)

	handle := handles.NewLocalVertexHandle(taskCh, ackCh)

	vid := s.GenerateVID()

	v := &InputOpCore{
		OpCore:  NewOpCore(vid, vertex.Type_Input, s),
		handle:  handle,
		inputCh: inputCh,
	}

	s.RegisterVertex(v, handle)
	return v
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
		case inDatum := <-op.inputCh:
			if err := op.handleInput(inDatum); err != nil {
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

func (op *InputOpCore) handleInput(inDatum request.InputDatum) error {
	msg := inDatum.Msg()
	ts := inDatum.Ts()

	if err := op.tsCheckAndUpdate(&ts); err != nil {
		return err
	}

	e := edge.NewEdge(op.id, op.target)
	if err := op.SendBy(e, msg, ts); err != nil {
		return err
	}
	return nil
}

func (op *InputOpCore) OnRecv(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error {
	return nil
}

func (op *InputOpCore) OnNotify(ts timestamp.Timestamp) error {
	return nil
}

func (op *InputOpCore) SendBy(e edge.Edge, msg *request.Message, ts timestamp.Timestamp) error {
	return op.coreSendBy(e, msg, ts, op.handle)
}

func (op *InputOpCore) NotifyAt(ts timestamp.Timestamp) error {
	return nil
}
