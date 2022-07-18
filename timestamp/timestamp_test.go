package timestamp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopyTimestampFrom(t *testing.T) {
	ts := NewTimestamp()
	ts.Counters = append(ts.Counters, 3)
	cts := CopyTimestampFrom(ts)

	assert.Equal(t, ts, cts)

	cts.Epoch = 1
	cts.Counters = append(cts.Counters, 6)

	assert.NotEqual(t, ts, cts)

	assert.Equal(t, ts.Epoch, 0)
	assert.Equal(t, len(ts.Counters), 2)
	assert.Equal(t, ts.Counters[0], 0)
	assert.Equal(t, ts.Counters[1], 3)
	assert.Equal(t, cts.Epoch, 1)
	assert.Equal(t, len(cts.Counters), 3)
	assert.Equal(t, cts.Counters[0], 0)
	assert.Equal(t, cts.Counters[1], 3)
	assert.Equal(t, cts.Counters[2], 6)
}

func TestLE(t *testing.T) {
	ts1 := NewTimestamp()
	ts1.Epoch = 1

	ts2 := NewTimestamp()
	ts2.Epoch = 2

	assert.Equal(t, LE(ts1, ts2), true)
	assert.Equal(t, LE(ts2, ts1), false)

	ts2.Epoch = 1
	assert.Equal(t, LE(ts1, ts2), true)
	assert.Equal(t, LE(ts2, ts1), true)

	ts1.Counters = append(ts1.Counters, 1)
	assert.Equal(t, LE(ts1, ts2), false)
	assert.Equal(t, LE(ts2, ts1), true)

	ts2.Counters = append(ts2.Counters, 2)
	assert.Equal(t, LE(ts1, ts2), true)
	assert.Equal(t, LE(ts2, ts1), false)

	ts1 = NewTimestamp()
	ts2 = NewTimestamp()
	assert.Equal(t, LE(ts1, ts2), true)
	assert.Equal(t, LE(ts2, ts1), true)

	ts2.Epoch = 1
	assert.Equal(t, LE(ts1, ts2), true)
	assert.Equal(t, LE(ts2, ts1), false)

	ts1.Epoch = 1
	ts1.Counters = append(ts1.Counters, 1)
	assert.Equal(t, LE(ts1, ts2), false)
	assert.Equal(t, LE(ts2, ts1), true)

	ts2.Counters = append(ts2.Counters, 1)
	assert.Equal(t, LE(ts1, ts2), true)
	assert.Equal(t, LE(ts2, ts1), true)

	ts1.Counters = append(ts1.Counters, 1)
	ts2.Counters = append(ts2.Counters, 2)
	assert.Equal(t, LE(ts1, ts2), true)
	assert.Equal(t, LE(ts2, ts1), false)
}
