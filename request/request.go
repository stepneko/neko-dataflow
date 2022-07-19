package request

import (
	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/timestamp"
)

type Type int

const (
	Type_SendBy   Type = 0 //  Function signature for SendBy
	Type_NotifyAt Type = 1 //  Function signature for NotifyAt
	Type_OnRecv   Type = 2 //  Function signature for OnRecv
	Type_OnNotify Type = 3 //  Function signature for OnNotify

	Type_IncreOC Type = 4 //  Function signature to increment occurrence count
	Type_DecreOC Type = 5 //  Function signature to decrement occurrence count
	Type_Ack     Type = 6 //  Function signature to ack
)

// Request represents a call between vertices and scheduler.
type Request struct {
	Type Type
	Edge edge.Edge
	Msg  Message
	Ts   timestamp.Timestamp
}
