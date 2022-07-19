package timestamp

import (
	"errors"
	"fmt"

	"github.com/stepneko/neko-dataflow/utils"
	"github.com/stepneko/neko-dataflow/vertex"
)

type Timestamp struct {
	Epoch    int
	Counters []int
}

func NewTimestamp() *Timestamp {
	return &Timestamp{
		Epoch:    0,
		Counters: []int{0},
	}
}

func NewTimestampWithParams(epoch int, counters []int) *Timestamp {
	return &Timestamp{
		Epoch:    epoch,
		Counters: counters,
	}
}

func CopyTimestampFrom(ts *Timestamp) *Timestamp {
	newCounters := make([]int, len(ts.Counters))
	copy(newCounters, ts.Counters)
	return &Timestamp{
		Epoch:    ts.Epoch,
		Counters: newCounters,
	}
}

func LE(a *Timestamp, b *Timestamp) bool {
	// First, check epoch
	if a.Epoch > b.Epoch {
		return false
	}

	// Then, check counters
	alen := len(a.Counters)
	blen := len(b.Counters)
	mlen := utils.Min(alen, blen)

	for i := 0; i < mlen; i++ {
		if a.Counters[i] < b.Counters[i] {
			return true
		}
		if a.Counters[i] > b.Counters[i] {
			return false
		}
	}

	return alen <= blen
}

func (ts *Timestamp) ToString() string {
	return fmt.Sprintf("Epoch: %d, Counters: %v", ts.Epoch, ts.Counters)
}

func HandleTimestamp(typ vertex.Type, ts *Timestamp) error {
	if typ == vertex.Type_Ingress {
		ts.Counters = append(ts.Counters, 0)
	} else if typ == vertex.Type_Egress {
		l := len(ts.Counters)
		if l == 0 {
			return errors.New("timestamp handling error in egress vertex. Counter already empty so cannot pop")
		}
		ts.Counters = ts.Counters[:l-1]
	} else if typ == vertex.Type_Feedback {
		l := len(ts.Counters)
		if l == 0 {
			return errors.New("timestamp handling error in feedback vertex. Counter already empty")
		}
		ts.Counters[l-1] += 1
	}
	return nil
}
