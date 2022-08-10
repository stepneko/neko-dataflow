package request

import (
	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/timestamp"
)

type Type int

const (
	Type_SendBy   Type = iota //  Function signature for SendBy
	Type_NotifyAt             //  Function signature for NotifyAt
	Type_OnRecv               //  Function signature for OnRecv
	Type_OnNotify             //  Function signature for OnNotify
	Type_IncreOC              //  Function signature to increment occurrence count
	Type_DecreOC              //  Function signature to decrement occurrence count
	Type_Ack                  //  Function signature to ack
)

// Request represents a call between vertices and scheduler.
type Request struct {
	Type Type
	Edge edge.Edge
	Msg  Message
	Ts   timestamp.Timestamp
}
