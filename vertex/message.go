package vertex

// Message represents the message payload to be transmitted between vertices.
// Data should be transmitted with protobuf serde.
type Message struct {
	data []byte
}

func NewMessage(data []byte) *Message {
	return &Message{
		data: data,
	}
}

// ToString is only a function for development and debugging use.
// In fact the bytes are used in protobuf format.
func (m *Message) ToString() string {
	return string(m.data[:])
}
