package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/vertex"
)

// Build a sample graph to test on, and return
// the array of vertices in positioned array.
//
// [v0]Input -> [v1]Ingress --> [v2]Generic -> [v3]Generic -> [v5]Egress -> [v6]Generic
//                                   ^                   |
//                                   |                   |
//                                   +-- [v4]Feedback  <-+
//
func BuildGraph(t *testing.T, g *Graph, vsp *[]vertex.Vertex) {
	*vsp = append(*vsp, vertex.NewInputVertex())
	*vsp = append(*vsp, vertex.NewIngressVertex())
	*vsp = append(*vsp, vertex.NewVertexCore())
	*vsp = append(*vsp, vertex.NewVertexCore())
	*vsp = append(*vsp, vertex.NewFeedbackVertex())
	*vsp = append(*vsp, vertex.NewEgressVertex())
	*vsp = append(*vsp, vertex.NewVertexCore())

	vs := *vsp

	// Insert the vertices into graph
	for index, v := range vs {
		v.SetId(constants.VertexId(index))
		g.InsertVertex(v)
	}

	g.InsertEdge(vertex.NewEdge(vs[0].GetId(), vs[1].GetId()), constants.VertexInDir_Default)
	g.InsertEdge(vertex.NewEdge(vs[1].GetId(), vs[2].GetId()), constants.VertexInDir_Default)
	g.InsertEdge(vertex.NewEdge(vs[2].GetId(), vs[3].GetId()), constants.VertexInDir_Default)
	g.InsertEdge(vertex.NewEdge(vs[3].GetId(), vs[4].GetId()), constants.VertexInDir_Default)
	g.InsertEdge(vertex.NewEdge(vs[4].GetId(), vs[2].GetId()), constants.VertexInDir_Default)
	g.InsertEdge(vertex.NewEdge(vs[3].GetId(), vs[5].GetId()), constants.VertexInDir_Default)
	g.InsertEdge(vertex.NewEdge(vs[5].GetId(), vs[6].GetId()), constants.VertexInDir_Default)

	assert.Equal(t, len(g.VertexMap[vs[0].GetId()].children), 1)
	assert.Equal(t, len(g.VertexMap[vs[1].GetId()].children), 1)
	assert.Equal(t, len(g.VertexMap[vs[2].GetId()].children), 1)
	assert.Equal(t, len(g.VertexMap[vs[3].GetId()].children), 2)
	assert.Equal(t, len(g.VertexMap[vs[4].GetId()].children), 1)
	assert.Equal(t, len(g.VertexMap[vs[5].GetId()].children), 1)
	assert.Equal(t, len(g.VertexMap[vs[6].GetId()].children), 0)
}

func TestInsertVertex(t *testing.T) {
	g := NewGraph()
	v := vertex.NewVertexCore()
	g.InsertVertex(v)

	node, exist := g.VertexMap[v.GetId()]
	assert.Equal(t, exist, true)
	assert.Equal(t, node.vertex, v)
}

func TestInsertEdge(t *testing.T) {
	g := NewGraph()
	v1 := vertex.NewGenericVertex()
	v1.SetId(1)
	g.InsertVertex(v1)
	v2 := vertex.NewIngressVertex()
	v2.SetId(2)
	g.InsertVertex(v2)
	e1 := vertex.NewEdge(v1.GetId(), v2.GetId())
	g.InsertEdge(e1, constants.VertexInDir_Default)

	node1, exist := g.VertexMap[v1.GetId()]
	assert.Equal(t, exist, true)
	assert.Equal(t, node1.vertex, v1)

	node2, exist := g.VertexMap[v2.GetId()]
	assert.Equal(t, exist, true)
	assert.Equal(t, node2.vertex, v2)

	assert.Equal(t, len(node1.children), 1)
	assert.Equal(t, len(node2.children), 0)

	assert.Equal(t, node1.children[node2], true)

	v3 := vertex.NewEgressVertex()
	v3.SetId(3)
	g.InsertVertex(v3)
	e2 := vertex.NewEdge(v2.GetId(), v3.GetId())
	g.InsertEdge(e2, constants.VertexInDir_Default)

	newNode2, exist := g.VertexMap[v2.GetId()]
	assert.Equal(t, exist, true)
	assert.Equal(t, newNode2, node2)
	assert.Equal(t, newNode2.vertex, v2)
	assert.Equal(t, len(node2.children), 1)

	node3, exists := g.VertexMap[v3.GetId()]
	assert.Equal(t, exists, true)
	assert.Equal(t, node3.vertex, v3)
	assert.Equal(t, len(node3.children), 0)

	assert.Equal(t, node2.children[node3], true)
}

func TestCouldResultIn(t *testing.T) {
	g := NewGraph()
	vsp := &[]vertex.Vertex{}
	BuildGraph(t, g, vsp)
	vs := *vsp

	// [(v0, v1), {0, []}] vs. [(v5, v6), {0, []}]
	ae := vertex.NewEdge(vs[0].GetId(), vs[1].GetId())
	ats := timestamp.NewTimestamp()
	aps := NewEdgePointStamp(ae, ats)
	be := vertex.NewEdge(vs[5].GetId(), vs[6].GetId())
	bts := timestamp.NewTimestamp()
	var bps Pointstamp = NewEdgePointStamp(be, bts)
	res, err := g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)

	// [(v0, v1), {1, []}] vs. [(v5, v6), {0, []}]
	ae = vertex.NewEdge(vs[0].GetId(), vs[1].GetId())
	ats = timestamp.NewTimestampWithParams(1, []int{})
	aps = NewEdgePointStamp(ae, ats)
	be = vertex.NewEdge(vs[5].GetId(), vs[6].GetId())
	bts = timestamp.NewTimestamp()
	bps = NewEdgePointStamp(be, bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)

	// [(v3, v5), {0, [5]}] vs. [(v5, v6), {0, []}]
	ae = vertex.NewEdge(vs[3].GetId(), vs[5].GetId())
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	be = vertex.NewEdge(vs[5].GetId(), vs[6].GetId())
	bts = timestamp.NewTimestamp()
	bps = NewEdgePointStamp(be, bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)

	// [(v3, v4), {0, [5]}] vs. [(v2, v3), {0, [5]}]
	ae = vertex.NewEdge(vs[3].GetId(), vs[4].GetId())
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	be = vertex.NewEdge(vs[2].GetId(), vs[3].GetId())
	bts = timestamp.NewTimestampWithParams(0, []int{5})
	bps = NewEdgePointStamp(be, bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)

	// [(v3, v4), {0, [5]}] vs. [(v2, v3), {0, [6]}]
	ae = vertex.NewEdge(vs[3].GetId(), vs[4].GetId())
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	be = vertex.NewEdge(vs[2].GetId(), vs[3].GetId())
	bts = timestamp.NewTimestampWithParams(0, []int{6})
	bps = NewEdgePointStamp(be, bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)

	// [(v2, v3), {0, [5]}] vs. [(v3, v3), {0, [5]}]
	ae = vertex.NewEdge(vs[2].GetId(), vs[3].GetId())
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	bts = timestamp.NewTimestampWithParams(0, []int{5})
	bps = NewVertexPointStamp(vs[3].GetId(), bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)

	// [(v2, v3), {0, [6]}] vs. [(v3, v3), {0, [5]}]
	ae = vertex.NewEdge(vs[2].GetId(), vs[3].GetId())
	ats = timestamp.NewTimestampWithParams(0, []int{6})
	aps = NewEdgePointStamp(ae, ats)
	bts = timestamp.NewTimestampWithParams(0, []int{5})
	bps = NewVertexPointStamp(vs[3].GetId(), bts)
	res, err = g.CouldResultIn(aps, bps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, false)
	res, err = g.CouldResultIn(bps, aps)
	assert.Equal(t, err, nil)
	assert.Equal(t, res, true)
}

func TestIncreOCAndDecreOC(t *testing.T) {
	g := NewGraph()
	vsp := &[]vertex.Vertex{}
	BuildGraph(t, g, vsp)
	vs := *vsp

	// Just increment OC for vs[0]
	ts := timestamp.NewTimestamp()
	ps1 := NewVertexPointStamp(vs[0].GetId(), ts)
	g.IncreOC(ps1)

	assert.Equal(t, len(g.ActivePsMap), 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].PC, 0)

	// Then increment another OC for edge[2, 3]
	ts = timestamp.NewTimestampWithParams(0, []int{5})
	e := vertex.NewEdge(vs[2].GetId(), vs[3].GetId())
	ps2 := NewEdgePointStamp(e, ts)
	g.IncreOC(ps2)
	assert.Equal(t, len(g.ActivePsMap), 2)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].PC, 1)

	// Increment another OC for edge[5, 6]
	ts = timestamp.NewTimestamp()
	e = vertex.NewEdge(vs[5].GetId(), vs[6].GetId())
	ps3 := NewEdgePointStamp(e, ts)
	g.IncreOC(ps3)
	assert.Equal(t, len(g.ActivePsMap), 3)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps2.Hash()].PC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.ActivePsMap[ps3.Hash()].PC, 2)

	// Increment yet another OC for edge[2, 3] with same pointstamp
	// Should not affect other pointstamps, just increase 1 OC
	ts = timestamp.NewTimestampWithParams(0, []int{5})
	e = vertex.NewEdge(vs[2].GetId(), vs[3].GetId())
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
	e = vertex.NewEdge(vs[2].GetId(), vs[3].GetId())
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
