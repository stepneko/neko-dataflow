package utils

import "github.com/stepneko/neko-dataflow/vertex"

type IdFactory interface {
	Generate() vertex.Id
}

type SimpleIdFactory struct {
	nextId vertex.Id
}

func NewSimpleIdFactory() *SimpleIdFactory {
	return &SimpleIdFactory{
		nextId: 1,
	}
}

func (f *SimpleIdFactory) Generate() vertex.Id {
	res := f.nextId
	f.nextId += 1
	return res
}
