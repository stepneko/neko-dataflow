package vertex

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopyTimestampFrom(t *testing.T) {
	ts := NewTimestamp()
	ts.counters = append(ts.counters, 3)
	cts := CopyTimestampFrom(ts)

	assert.Equal(t, ts, cts)

	cts.epoch = 1
	cts.counters = append(cts.counters, 6)

	assert.NotEqual(t, ts, cts)

	assert.Equal(t, ts.epoch, 0)
	assert.Equal(t, len(ts.counters), 1)
	assert.Equal(t, ts.counters[0], 3)
	assert.Equal(t, cts.epoch, 1)
	assert.Equal(t, len(cts.counters), 2)
	assert.Equal(t, cts.counters[0], 3)
	assert.Equal(t, cts.counters[1], 6)
}

func TestLessThan(t *testing.T) {
	ts1 := NewTimestamp()
	ts1.epoch = 1

	ts2 := NewTimestamp()
	ts2.epoch = 2

	assert.Equal(t, LessThan(ts1, ts2), true)
	assert.Equal(t, LessThan(ts2, ts1), false)

	ts2.epoch = 1
	assert.Equal(t, LessThan(ts1, ts2), false)
	assert.Equal(t, LessThan(ts2, ts1), false)

	ts1.counters = append(ts1.counters, 1)
	assert.Equal(t, LessThan(ts1, ts2), false)
	assert.Equal(t, LessThan(ts2, ts1), true)

	ts2.counters = append(ts2.counters, 2)
	assert.Equal(t, LessThan(ts1, ts2), true)
	assert.Equal(t, LessThan(ts2, ts1), false)
}

func TestEqual(t *testing.T) {
	ts1 := NewTimestamp()
	ts2 := NewTimestamp()
	assert.Equal(t, Equal(ts1, ts2), true)
	assert.Equal(t, Equal(ts2, ts1), true)

	ts2.epoch = 1
	assert.Equal(t, Equal(ts1, ts2), false)
	assert.Equal(t, Equal(ts2, ts1), false)

	ts1.epoch = 1
	ts1.counters = append(ts1.counters, 1)
	assert.Equal(t, Equal(ts1, ts2), false)
	assert.Equal(t, Equal(ts2, ts1), false)

	ts2.counters = append(ts2.counters, 1)
	assert.Equal(t, Equal(ts1, ts2), true)
	assert.Equal(t, Equal(ts2, ts1), true)

	ts1.counters = append(ts1.counters, 1)
	ts2.counters = append(ts2.counters, 2)
	assert.Equal(t, Equal(ts1, ts2), false)
	assert.Equal(t, Equal(ts2, ts1), false)
}
