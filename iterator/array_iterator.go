package iterator

import "errors"

type ArrayIterator[T any] struct {
	arr []T
	ptr int
}

func IterFromArray[T any](arr []T) *ArrayIterator[T] {
	return &ArrayIterator[T]{
		arr: arr,
		ptr: 0,
	}
}

func (iter *ArrayIterator[T]) Iter() (T, error) {
	var defval T
	if iter.ptr < 0 {
		return defval, errors.New("ptr cannot be < 0 in array iterator")
	}
	if iter.ptr >= len(iter.arr) {
		return defval, errors.New("ptr >= len of array in array iterator")
	}

	t := iter.arr[iter.ptr]
	iter.ptr += 1
	return t, nil
}

func (iter *ArrayIterator[T]) HasElement() (bool, error) {
	if iter.ptr < 0 {
		return false, errors.New("ptr cannot be < 0 in array iterator")
	}
	if iter.ptr > len(iter.arr) {
		return false, errors.New("ptr > len of array in array iterator")
	}
	if iter.ptr == len(iter.arr) {
		return false, nil
	}
	return true, nil
}
