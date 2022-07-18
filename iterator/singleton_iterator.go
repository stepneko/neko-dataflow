package iterator

import "errors"

type SingletonIterator[T any] struct {
	item T
	flag bool
}

func IterFromSingleton[T any](item T) *SingletonIterator[T] {
	return &SingletonIterator[T]{
		item: item,
		flag: true,
	}
}

func (iter *SingletonIterator[T]) Iter() (T, error) {
	var defval T
	if !iter.flag {
		return defval, errors.New("flag is false in singleton iterator")
	}
	iter.flag = false
	return iter.item, nil
}

func (iter *SingletonIterator[T]) HasElement() (bool, error) {
	if !iter.flag {
		return false, nil
	}
	return true, nil
}
