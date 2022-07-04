package vertex

import (
	"github.com/stepneko/neko-dataflow/timestamp"
)

// Request represents a call between vertices and scheduler.
type Request struct {
	typ CallbackType
	e   Edge
	ts  timestamp.Timestamp
	m   Message
}

func NewRequest(
	typ CallbackType,
	e Edge,
	ts timestamp.Timestamp,
	m Message,
) *Request {
	return &Request{
		typ: typ,
		e:   e,
		ts:  ts,
		m:   m,
	}
}

func (r *Request) GetType() CallbackType {
	return r.typ
}

func (r *Request) GetEdge() Edge {
	return r.e
}

func (r *Request) GetTimestamp() timestamp.Timestamp {
	return r.ts
}

func (r *Request) GetMessage() Message {
	return r.m
}
