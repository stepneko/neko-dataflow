package vertex

// Request represents a call between vertices and scheduler.
type Request struct {
	typ CallbackType
	e   Edge
	ts  Timestamp
	msg Message
}

func NewRequest(
	typ CallbackType,
	e Edge,
	ts Timestamp,
	msg Message,
) *Request {
	return &Request{
		typ: typ,
		e:   e,
		ts:  ts,
		msg: msg,
	}
}

func (r *Request) GetType() CallbackType {
	return r.typ
}

func (r *Request) GetEdge() Edge {
	return r.e
}

func (r *Request) GetTimestamp() Timestamp {
	return r.ts
}

func (r *Request) GetMessage() Message {
	return r.msg
}
