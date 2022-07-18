package graph

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/timestamp"
)

// Build a sample graph to test on, and return
// the array of vertices in positioned array.
//
// [v1]Input -> [v2]Ingress --> [v3]Generic -> [v4]Generic -> [v6]Egress -> [v7]Generic
//                                   ^                   |
//                                   |                   |
//                                   +-- [v5]Feedback  <-+
//
func BuildGraph(t *testing.T, g *Graph) {
	// Insert vertices
	g.InsertVertex(1, constants.VertexType_Input)
	g.InsertVertex(2, constants.VertexType_Ingress)
	g.InsertVertex(3, constants.VertexType_Inspect)
	g.InsertVertex(4, constants.VertexType_Inspect)
	g.InsertVertex(5, constants.VertexType_Feedback)
	g.InsertVertex(6, constants.VertexType_Egress)
	g.InsertVertex(7, constants.VertexType_Inspect)

	// Insert edge
	g.InsertEdge(edge.NewEdge(1, 2))
	g.InsertEdge(edge.NewEdge(2, 3))
	g.InsertEdge(edge.NewEdge(3, 4))
	g.InsertEdge(edge.NewEdge(4, 5))
	g.InsertEdge(edge.NewEdge(5, 3))
	g.InsertEdge(edge.NewEdge(4, 6))
	g.InsertEdge(edge.NewEdge(6, 7))

	assert.Equal(t, len(g.VertexMap[1].Children), 1)
	assert.Equal(t, len(g.VertexMap[2].Children), 1)
	assert.Equal(t, len(g.VertexMap[3].Children), 1)
	assert.Equal(t, len(g.VertexMap[4].Children), 2)
	assert.Equal(t, len(g.VertexMap[5].Children), 1)
	assert.Equal(t, len(g.VertexMap[6].Children), 1)
	assert.Equal(t, len(g.VertexMap[7].Children), 0)
}

func TestInsertVertex(t *testing.T) {
	g := NewGraph()
	vid := constants.VertexId(1)
	g.InsertVertex(vid, constants.VertexType_Input)

	node, exist := g.VertexMap[vid]
	assert.Equal(t, exist, true)
	assert.Equal(t, node.Vid, vid)
	assert.Equal(t, node.Typ, constants.VertexType_Input)
}

func TestInsertEdge(t *testing.T) {
	g := NewGraph()
	vid1 := constants.VertexId(1)
	vid2 := constants.VertexId(2)
	g.InsertVertex(vid1, constants.VertexType_Input)
	g.InsertVertex(vid2, constants.VertexType_Ingress)
	g.InsertEdge(edge.NewEdge(vid1, vid2))

	node1, exist := g.VertexMap[vid1]
	assert.Equal(t, exist, true)
	assert.Equal(t, node1.Vid, vid1)
	assert.Equal(t, node1.Typ, constants.VertexType_Input)

	node2, exist := g.VertexMap[vid2]
	assert.Equal(t, exist, true)
	assert.Equal(t, node2.Vid, vid2)
	assert.Equal(t, node2.Typ, constants.VertexType_Ingress)

	assert.Equal(t, len(node1.Children), 1)
	assert.Equal(t, len(node2.Children), 0)

	assert.Equal(t, node1.Children[node2], true)

	vid3 := constants.VertexId(3)
	g.InsertVertex(vid3, constants.VertexType_Egress)
	g.InsertEdge(edge.NewEdge(vid2, vid3))

	newNode2, exist := g.VertexMap[vid2]
	assert.Equal(t, exist, true)
	assert.Equal(t, newNode2, node2)
	assert.Equal(t, newNode2.Vid, vid2)
	assert.Equal(t, node2.Typ, constants.VertexType_Ingress)
	assert.Equal(t, len(node2.Children), 1)

	node3, exists := g.VertexMap[vid3]
	assert.Equal(t, exists, true)
	assert.Equal(t, node3.Vid, vid3)
	assert.Equal(t, node3.Typ, constants.VertexType_Egress)
	assert.Equal(t, len(node3.Children), 0)

	assert.Equal(t, node2.Children[node3], true)
}

func TestCouldResultIn(t *testing.T) {
	g := NewGraph()
	BuildGraph(t, g)

	// [(v1, v2), {0, []}] vs. [(v6, v7), {0, []}]
	ae := edge.NewEdge(1, 2)
	ats := timestamp.NewTimestamp()
	aps := NewEdgePointStamp(ae, ats)
	be := edge.NewEdge(6, 7)
	bts := timestamp.NewTimestamp()
	var bps Pointstamp = NewEdgePointStamp(be, bts)
	res, err := g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)

	// [(v1, v2), {1, []}] vs. [(v6, v7), {0, []}]
	ae = edge.NewEdge(1, 2)
	ats = timestamp.NewTimestampWithParams(1, []int{})
	aps = NewEdgePointStamp(ae, ats)
	be = edge.NewEdge(6, 7)
	bts = timestamp.NewTimestamp()
	bps = NewEdgePointStamp(be, bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)

	// [(v4, v6), {0, [5]}] vs. [(v6, v7), {0, []}]
	ae = edge.NewEdge(4, 6)
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	be = edge.NewEdge(6, 7)
	bts = timestamp.NewTimestamp()
	bps = NewEdgePointStamp(be, bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)

	// [(v4, v5), {0, [5]}] vs. [(v3, v4), {0, [5]}]
	ae = edge.NewEdge(4, 5)
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	be = edge.NewEdge(3, 4)
	bts = timestamp.NewTimestampWithParams(0, []int{5})
	bps = NewEdgePointStamp(be, bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)

	// [(v4, v5), {0, [5]}] vs. [(v3, v4), {0, [6]}]
	ae = edge.NewEdge(4, 5)
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	be = edge.NewEdge(3, 4)
	bts = timestamp.NewTimestampWithParams(0, []int{6})
	bps = NewEdgePointStamp(be, bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)

	// [(v3, v4), {0, [5]}] vs. [(v4, v4), {0, [5]}]
	ae = edge.NewEdge(3, 4)
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	bts = timestamp.NewTimestampWithParams(0, []int{5})
	bps = NewVertexPointStamp(4, bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)

	// [(v3, v4), {0, [6]}] vs. [(v4, v4), {0, [5]}]
	ae = edge.NewEdge(3, 4)
	ats = timestamp.NewTimestampWithParams(0, []int{6})
	aps = NewEdgePointStamp(ae, ats)
	bts = timestamp.NewTimestampWithParams(0, []int{5})
	bps = NewVertexPointStamp(4, bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)
}

func TestIncreOCAndDecreOC(t *testing.T) {
	g := NewGraph()
	BuildGraph(t, g)

	// Just increment OC for v1
	ts := timestamp.NewTimestamp()
	ps1 := NewVertexPointStamp(1, ts)
	g.IncreOC(ps1)

	assert.Equal(t, len(g.ActivePsMap), 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].PC, 0)

	// Then increment another OC for edge[3, 4]
	ts = timestamp.NewTimestampWithParams(0, []int{5})
	e := edge.NewEdge(3, 4)
	ps2 := NewEdgePointStamp(e, ts)
	g.IncreOC(ps2)
	assert.Equal(t, len(g.ActivePsMap), 2)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].PC, 1)

	// Increment another OC for edge[6, 7]
	ts = timestamp.NewTimestamp()
	e = edge.NewEdge(6, 7)
	ps3 := NewEdgePointStamp(e, ts)
	g.IncreOC(ps3)
	assert.Equal(t, len(g.ActivePsMap), 3)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].PC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].PC, 2)

	// Increment yet another OC for edge[3, 4] with same pointstamp
	// Should not affect other pointstamps, just increase 1 OC
	ts = timestamp.NewTimestampWithParams(0, []int{5})
	e = edge.NewEdge(3, 4)
	ps4 := NewEdgePointStamp(e, ts)
	g.IncreOC(ps4)
	assert.Equal(t, len(g.ActivePsMap), 3)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].OC, 2)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].PC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].PC, 2)

	// Increment a pointstamp that is most recent
	ts = timestamp.NewTimestampWithParams(1, []int{6})
	e = edge.NewEdge(3, 4)
	ps5 := NewEdgePointStamp(e, ts)
	g.IncreOC(ps5)
	assert.Equal(t, len(g.ActivePsMap), 4)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].OC, 2)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].PC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].PC, 2)
	assert.Equal(t, g.ActivePsMap[ps5.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps5.Hash()].PC, 2)

	// Decrement ps4 a.k.a. ps2
	// Should not affect other pointstamps, just decrease 1 OC
	g.DecreOC(ps4)
	assert.Equal(t, len(g.ActivePsMap), 4)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].PC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].PC, 2)
	assert.Equal(t, g.ActivePsMap[ps5.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps5.Hash()].PC, 2)

	// Decrement ps2 again
	g.DecreOC(ps2)
	assert.Equal(t, len(g.ActivePsMap), 3)
	_, exist := g.ActivePsMap[ps2.Hash()]
	assert.Equal(t, exist, false)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].PC, 1)
	assert.Equal(t, g.ActivePsMap[ps5.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps5.Hash()].PC, 1)

	// Decrement ps1
	g.DecreOC(ps1)
	assert.Equal(t, len(g.ActivePsMap), 2)
	_, exist = g.ActivePsMap[ps1.Hash()]
	assert.Equal(t, exist, false)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].PC, 0)
	assert.Equal(t, g.ActivePsMap[ps5.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps5.Hash()].PC, 0)

	// Decrement ps3
	g.DecreOC(ps3)
	assert.Equal(t, len(g.ActivePsMap), 1)
	_, exist = g.ActivePsMap[ps3.Hash()]
	assert.Equal(t, exist, false)
	assert.Equal(t, g.ActivePsMap[ps5.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps5.Hash()].PC, 0)

	// Decrement ps5
	g.DecreOC(ps5)
	assert.Equal(t, len(g.ActivePsMap), 0)
	_, exist = g.ActivePsMap[ps5.Hash()]
	assert.Equal(t, exist, false)
}
