package vertex

import (
	"github.com/stepneko/neko-dataflow/timestamp"
)

type CallbackType int

const (
	CallbackType_SendBy   CallbackType = 0 // Callback function signature for SendBy
	CallbackType_NotifyAt CallbackType = 1 // Callback function signature for NotifyAt
	CallbackType_OnRecv   CallbackType = 2 // Callback function signature for OnRecv
	CallbackType_OnNotify CallbackType = 3 // Callback function signature for OnNotify

	CallbackType_IncreOC CallbackType = 4 // Callback function signature to increment occurrence count
	CallbackType_DecreOC CallbackType = 5 // Callback function signature to decrement occurrence count
	CallbackType_Ack     CallbackType = 6 // Callback function signature to ack
)

// Callback is the signature for basic vertex functions such as
// SendBy, NotifyAt, OnRecv, OnNotify and timestamp processing.
// For SendBy and OnRecv, all three parameters are needed.
// For NotifyAt and OnNotify, m Message could be nil since it's not used.
// For timestamp handling, both e Edge and m Message could be nil.
type Callback func(e Edge, m Message, ts timestamp.Timestamp) error
