package request

import "github.com/stepneko/neko-dataflow/timestamp"

type InputDatum interface {
	Msg() *Message
	Ts() timestamp.Timestamp
}

type InputRaw struct {
	msg *Message
	ts  timestamp.Timestamp
}

func NewInputRaw(msg *Message, ts timestamp.Timestamp) *InputRaw {
	return &InputRaw{
		msg: msg,
		ts:  ts,
	}
}

func (ir *InputRaw) Msg() *Message {
	return ir.msg
}

func (ir *InputRaw) Ts() timestamp.Timestamp {
	return ir.ts
}
