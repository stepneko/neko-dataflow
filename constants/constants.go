package constants

const ChanCacapity int = 1024

type VertexId int

type VertexType int

const (
	VertexType_Generic  VertexType = 1
	VertexType_Ingress  VertexType = 2
	VertexType_Egress   VertexType = 3
	VertexType_Feedback VertexType = 4
	VertexType_Input    VertexType = 5
	VertexType_Unary    VertexType = 6
	VertexType_Bianry   VertexType = 7
)

const VertexInDirs int = 2

type VertexInDir int

const (
	VertexInDir_Default VertexInDir = 0 // Default vertex inward direction, pointing to left
	VertexInDir_Left    VertexInDir = 0 // Pointing to left of target vertex
	VertexInDir_Right   VertexInDir = 1 // Pointing to right of target vertex
)

type RequestType int

const (
	RequestType_SendBy   RequestType = 0 //  Function signature for SendBy
	RequestType_NotifyAt RequestType = 1 //  Function signature for NotifyAt
	RequestType_OnRecv   RequestType = 2 //  Function signature for OnRecv
	RequestType_OnNotify RequestType = 3 //  Function signature for OnNotify

	RequestType_IncreOC RequestType = 4 //  Function signature to increment occurrence count
	RequestType_DecreOC RequestType = 5 //  Function signature to decrement occurrence count
	RequestType_Ack     RequestType = 6 //  Function signature to ack
)
