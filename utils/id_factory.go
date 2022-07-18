package utils

import (
	"github.com/stepneko/neko-dataflow/constants"
)

type IdFactory interface {
	Generate() constants.VertexId
}

type SimpleIdFactory struct {
	nextId constants.VertexId
}

func NewSimpleIdFactory() *SimpleIdFactory {
	return &SimpleIdFactory{
		nextId: 1,
	}
}

func (f *SimpleIdFactory) Generate() constants.VertexId {
	res := f.nextId
	f.nextId += 1
	return res
}
