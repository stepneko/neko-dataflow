package vertex

// Message represents the message payload to be transmitted between vertices.
// Data should be transmitted with protobuf serde.
type Message struct {
	data []byte
}
