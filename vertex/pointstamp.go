package vertex

// Pointstamp, as referred to in the paper, is a tuple with two types:
// (Vertex, Timestamp) and (Edge, Timestamp).
// To generalize the above types and to make could-result-in inference,
// We use Pointstamp as the interface, for which one must could get
// src, target and timestamp for graph traversal.
type Pointstamp interface {
	GetSrc() Vertex
	GetTarget() Vertex
	GetTimestamp() *Timestamp
}

// VertexPointStamp is a vertex based pointstamp
type VertexPointStamp struct {
	vertex Vertex
	ts     *Timestamp
}

func NewVertexPointStamp(
	v Vertex,
	ts *Timestamp,
) *VertexPointStamp {
	return &VertexPointStamp{
		vertex: v,
		ts:     ts,
	}
}

func (vps *VertexPointStamp) GetSrc() Vertex {
	return vps.vertex
}

func (vps *VertexPointStamp) GetTarget() Vertex {
	return vps.vertex
}

func (vps *VertexPointStamp) GetTimestamp() *Timestamp {
	return vps.ts
}

// EdgePointStamp is an edge based pointstamp
type EdgePointStamp struct {
	edge Edge
	ts   *Timestamp
}

func NewEdgePointStamp(
	e Edge,
	ts *Timestamp,
) *EdgePointStamp {
	return &EdgePointStamp{
		edge: e,
		ts:   ts,
	}
}

func (eps *EdgePointStamp) GetSrc() Vertex {
	return eps.edge.GetSrc()
}

func (eps *EdgePointStamp) GetTarget() Vertex {
	return eps.edge.GetTarget()
}

func (eps *EdgePointStamp) GetTimestamp() *Timestamp {
	return eps.ts
}
