package iterator

import (
	"fmt"
	"testing"

	"github.com/stepneko/neko-dataflow/request"
	"github.com/stretchr/testify/assert"
)

func TestArrayIteratorWithPointer(t *testing.T) {
	arr := []*request.Message{}
	for i := 0; i < 100; i++ {
		arr = append(arr, request.NewMessage([]byte(fmt.Sprintf("message %d", i))))
	}

	iter := IterFromArray(arr)

	for i := 0; i < 100; i++ {
		flag, err := iter.HasElement()
		assert.Equal(t, flag, true)
		assert.Equal(t, err, nil)
		msg, err := iter.Iter()
		assert.Equal(t, msg.ToString(), fmt.Sprintf("message %d", i))
		assert.Equal(t, err, nil)
	}

	flag, err := iter.HasElement()
	assert.Equal(t, flag, false)
	assert.Equal(t, err, nil)

	_, err = iter.Iter()
	assert.Equal(t, err.Error(), "ptr >= len of array in array iterator")

	iter.ptr += 1
	flag, err = iter.HasElement()
	assert.Equal(t, flag, false)
	assert.Equal(t, err.Error(), "ptr > len of array in array iterator")

	_, err = iter.Iter()
	assert.Equal(t, err.Error(), "ptr >= len of array in array iterator")

	iter.ptr = -1
	flag, err = iter.HasElement()
	assert.Equal(t, flag, false)
	assert.Equal(t, err.Error(), "ptr cannot be < 0 in array iterator")

	_, err = iter.Iter()
	assert.Equal(t, err.Error(), "ptr cannot be < 0 in array iterator")
}

func TestArrayIteratorWithValue(t *testing.T) {
	arr := []request.Message{}
	for i := 0; i < 100; i++ {
		arr = append(arr, *request.NewMessage([]byte(fmt.Sprintf("message %d", i))))
	}

	iter := IterFromArray(arr)

	for i := 0; i < 100; i++ {
		flag, err := iter.HasElement()
		assert.Equal(t, flag, true)
		assert.Equal(t, err, nil)
		msg, err := iter.Iter()
		assert.Equal(t, msg.ToString(), fmt.Sprintf("message %d", i))
		assert.Equal(t, err, nil)
	}

	flag, err := iter.HasElement()
	assert.Equal(t, flag, false)
	assert.Equal(t, err, nil)

	_, err = iter.Iter()
	assert.Equal(t, err.Error(), "ptr >= len of array in array iterator")

	iter.ptr += 1
	flag, err = iter.HasElement()
	assert.Equal(t, flag, false)
	assert.Equal(t, err.Error(), "ptr > len of array in array iterator")

	_, err = iter.Iter()
	assert.Equal(t, err.Error(), "ptr >= len of array in array iterator")

	iter.ptr = -1
	flag, err = iter.HasElement()
	assert.Equal(t, flag, false)
	assert.Equal(t, err.Error(), "ptr cannot be < 0 in array iterator")

	_, err = iter.Iter()
	assert.Equal(t, err.Error(), "ptr cannot be < 0 in array iterator")
}
