package vertex

import (
	"github.com/stepneko/neko-dataflow/utils"
)

type Timestamp struct {
	epoch    int
	counters []int
}

func NewTimestamp() *Timestamp {
	return &Timestamp{
		epoch:    0,
		counters: []int{},
	}
}

func CopyTimestampFrom(ts *Timestamp) *Timestamp {
	newCounters := make([]int, len(ts.counters))
	copy(newCounters, ts.counters)
	return &Timestamp{
		epoch:    ts.epoch,
		counters: newCounters,
	}
}

func LessThan(a *Timestamp, b *Timestamp) bool {
	// First, check epoch
	if a.epoch < b.epoch {
		return true
	}
	if a.epoch > b.epoch {
		return false
	}

	// Then, check counters
	alen := len(a.counters)
	blen := len(b.counters)
	mlen := utils.Min(alen, blen)

	for i := 0; i < mlen; i++ {
		if a.counters[i] < b.counters[i] {
			return true
		}
		if a.counters[i] > b.counters[i] {
			return false
		}
	}
	return alen < blen
}

func Equal(a *Timestamp, b *Timestamp) bool {
	if a.epoch != b.epoch {
		return false
	}

	alen := len(a.counters)
	blen := len(b.counters)

	if alen != blen {
		return false
	}

	for i := 0; i < alen; i++ {
		if a.counters[i] != b.counters[i] {
			return false
		}
	}
	return true
}
