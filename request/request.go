package request

import (
	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/timestamp"
)

// Request represents a call between vertices and scheduler.
type Request struct {
	Typ  constants.RequestType
	Edge edge.Edge
	Msg  Message
	Ts   timestamp.Timestamp
}
