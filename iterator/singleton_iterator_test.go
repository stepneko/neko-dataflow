package iterator

import (
	"testing"

	"github.com/stepneko/neko-dataflow/request"
	"github.com/stretchr/testify/assert"
)

func TestSingletonIteratorWithPointer(t *testing.T) {
	item := request.NewMessage([]byte("message"))

	iter := IterFromSingleton(item)

	flag, err := iter.HasElement()
	assert.Equal(t, flag, true)
	assert.Equal(t, err, nil)

	msg, err := iter.Iter()
	assert.Equal(t, msg.ToString(), "message")
	assert.Equal(t, err, nil)

	flag, err = iter.HasElement()
	assert.Equal(t, flag, false)
	assert.Equal(t, err, nil)

	_, err = iter.Iter()
	assert.Equal(t, err.Error(), "flag is false in singleton iterator")
}

func TestSingletonIteratorWithValue(t *testing.T) {
	item := *request.NewMessage([]byte("message"))

	iter := IterFromSingleton(item)

	flag, err := iter.HasElement()
	assert.Equal(t, flag, true)
	assert.Equal(t, err, nil)

	msg, err := iter.Iter()
	assert.Equal(t, msg.ToString(), "message")
	assert.Equal(t, err, nil)

	flag, err = iter.HasElement()
	assert.Equal(t, flag, false)
	assert.Equal(t, err, nil)

	_, err = iter.Iter()
	assert.Equal(t, err.Error(), "flag is false in singleton iterator")
}
