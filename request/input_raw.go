package request

import "github.com/stepneko/neko-dataflow/timestamp"

type InputRaw struct {
	Msg Message
	Ts  timestamp.Timestamp
}
