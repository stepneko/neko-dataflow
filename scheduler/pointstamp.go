package scheduler

import (
	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/utils"
	"github.com/stepneko/neko-dataflow/vertex"
)

// Pointstamp, as referred to in the paper, is a tuple with two types:
// (Vertex, Timestamp) and (Edge, Timestamp).
// To generalize the above types and to make could-result-in inference,
// We use Pointstamp as the interface, for which one must could get
// src, target and timestamp for graph traversal.
type Pointstamp interface {
	GetSrc() constants.VertexId
	GetTarget() constants.VertexId
	GetTimestamp() *timestamp.Timestamp
	Hash() string
}

type PointstampCounter struct {
	PS Pointstamp
	OC int
	PC int
}

// VertexPointStamp is a vertex based pointstamp
type VertexPointStamp struct {
	vertexId constants.VertexId
	ts       *timestamp.Timestamp
}

func NewVertexPointStamp(
	vertexId constants.VertexId,
	ts *timestamp.Timestamp,
) *VertexPointStamp {
	return &VertexPointStamp{
		vertexId: vertexId,
		ts:       ts,
	}
}

func (vps *VertexPointStamp) GetSrc() constants.VertexId {
	return vps.vertexId
}

func (vps *VertexPointStamp) GetTarget() constants.VertexId {
	return vps.vertexId
}

func (vps *VertexPointStamp) GetTimestamp() *timestamp.Timestamp {
	return vps.ts
}

func (vps *VertexPointStamp) Hash() string {
	vertexHash := utils.Hash(vps.vertexId)
	tsHash := utils.Hash(vps.ts)
	return utils.Hash(vertexHash + tsHash)
}

// EdgePointStamp is an edge based pointstamp
type EdgePointStamp struct {
	edge vertex.Edge
	ts   *timestamp.Timestamp
}

func NewEdgePointStamp(
	e vertex.Edge,
	ts *timestamp.Timestamp,
) *EdgePointStamp {
	return &EdgePointStamp{
		edge: e,
		ts:   ts,
	}
}

func (eps *EdgePointStamp) GetSrc() constants.VertexId {
	return eps.edge.GetSrc()
}

func (eps *EdgePointStamp) GetTarget() constants.VertexId {
	return eps.edge.GetTarget()
}

func (eps *EdgePointStamp) GetTimestamp() *timestamp.Timestamp {
	return eps.ts
}

func (eps *EdgePointStamp) Hash() string {
	srcHash := utils.Hash(eps.edge.GetSrc())
	targetHash := utils.Hash(eps.edge.GetTarget())
	edgeHash := utils.Hash(srcHash + targetHash)
	tsHash := utils.Hash(eps.ts)
	return utils.Hash(edgeHash + tsHash)
}
