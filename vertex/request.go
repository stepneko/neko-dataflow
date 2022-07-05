package vertex

import (
	"github.com/stepneko/neko-dataflow/timestamp"
)

// Request represents a call between vertices and scheduler.
type Request struct {
	Typ  CallbackType
	Edge Edge
	Ts   timestamp.Timestamp
	Msg  Message
}
