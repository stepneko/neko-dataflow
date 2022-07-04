package timestamp

import (
	"fmt"

	"github.com/stepneko/neko-dataflow/utils"
)

type Timestamp struct {
	Epoch    int
	Counters []int
}

func NewTimestamp() *Timestamp {
	return &Timestamp{
		Epoch:    0,
		Counters: []int{},
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
