package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stepneko/neko-dataflow/timestamp"
	"github.com/stepneko/neko-dataflow/vertex"
)

// Build a sample graph to test on, and return
// the array of vertices in positioned array.
//
// [v0]Generic -> [v1]Ingress --> [v2]Generic -> [v3]Generic -> [v5]Egress -> [v6]Generic
//                                   ^                   |
//                                   |                   |
//                                   +-- [v4]Feedback  <-+
//
func BuildGraph(t *testing.T, g *Graph, vsp *[]vertex.Vertex) {
	*vsp = append(*vsp, vertex.NewGenericVertex())
	*vsp = append(*vsp, vertex.NewIngressVertex())
	*vsp = append(*vsp, vertex.NewGenericVertex())
	*vsp = append(*vsp, vertex.NewGenericVertex())
	*vsp = append(*vsp, vertex.NewFeedbackVertex())
	*vsp = append(*vsp, vertex.NewEgressVertex())
	*vsp = append(*vsp, vertex.NewGenericVertex())

	vs := *vsp

	// Insert the vertices into graph
	for _, v := range vs {
		g.InsertVertex(v)
	}

	g.InsertEdge(vertex.NewEdge(vs[0], vs[1]))
	g.InsertEdge(vertex.NewEdge(vs[1], vs[2]))
	g.InsertEdge(vertex.NewEdge(vs[2], vs[3]))
	g.InsertEdge(vertex.NewEdge(vs[3], vs[4]))
	g.InsertEdge(vertex.NewEdge(vs[4], vs[2]))
	g.InsertEdge(vertex.NewEdge(vs[3], vs[5]))
	g.InsertEdge(vertex.NewEdge(vs[5], vs[6]))

	assert.Equal(t, len(g.vertexMap[vs[0]].children), 1)
	assert.Equal(t, len(g.vertexMap[vs[1]].children), 1)
	assert.Equal(t, len(g.vertexMap[vs[2]].children), 1)
	assert.Equal(t, len(g.vertexMap[vs[3]].children), 2)
	assert.Equal(t, len(g.vertexMap[vs[4]].children), 1)
	assert.Equal(t, len(g.vertexMap[vs[5]].children), 1)
	assert.Equal(t, len(g.vertexMap[vs[6]].children), 0)
}

func TestInsertVertex(t *testing.T) {
	g := NewGraph()
	v := vertex.NewGenericVertex()
	g.InsertVertex(v)

	node, exist := g.vertexMap[v]
	assert.Equal(t, exist, true)
	assert.Equal(t, node.vertex, v)
}

func TestInsertEdge(t *testing.T) {
	g := NewGraph()
	v1 := vertex.NewGenericVertex()
	v2 := vertex.NewIngressVertex()
	e1 := vertex.NewEdge(v1, v2)
	g.InsertEdge(e1)

	node1, exist := g.vertexMap[v1]
	assert.Equal(t, exist, true)
	assert.Equal(t, node1.vertex, v1)

	node2, exist := g.vertexMap[v2]
	assert.Equal(t, exist, true)
	assert.Equal(t, node2.vertex, v2)

	assert.Equal(t, len(node1.children), 1)
	assert.Equal(t, len(node2.children), 0)

	assert.Equal(t, node1.children[node2], true)

	v3 := vertex.NewEgressVertex()
	e2 := vertex.NewEdge(v2, v3)
	g.InsertEdge(e2)

	newNode2, exist := g.vertexMap[v2]
	assert.Equal(t, exist, true)
	assert.Equal(t, newNode2, node2)
	assert.Equal(t, newNode2.vertex, v2)
	assert.Equal(t, len(node2.children), 1)

	node3, exists := g.vertexMap[v3]
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
	ae := vertex.NewEdge(vs[0], vs[1])
	ats := timestamp.NewTimestamp()
	aps := NewEdgePointStamp(ae, ats)
	be := vertex.NewEdge(vs[5], vs[6])
	bts := timestamp.NewTimestamp()
	var bps Pointstamp = NewEdgePointStamp(be, bts)
	assert.Equal(t, g.CouldResultIn(aps, bps), true)
	assert.Equal(t, g.CouldResultIn(bps, aps), false)

	// [(v0, v1), {1, []}] vs. [(v5, v6), {0, []}]
	ae = vertex.NewEdge(vs[0], vs[1])
	ats = timestamp.NewTimestampWithParams(1, []int{})
	aps = NewEdgePointStamp(ae, ats)
	be = vertex.NewEdge(vs[5], vs[6])
	bts = timestamp.NewTimestamp()
	bps = NewEdgePointStamp(be, bts)
	assert.Equal(t, g.CouldResultIn(aps, bps), false)
	assert.Equal(t, g.CouldResultIn(bps, aps), false)

	// [(v3, v5), {0, [5]}] vs. [(v5, v6), {0, []}]
	ae = vertex.NewEdge(vs[3], vs[5])
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	be = vertex.NewEdge(vs[5], vs[6])
	bts = timestamp.NewTimestamp()
	bps = NewEdgePointStamp(be, bts)
	assert.Equal(t, g.CouldResultIn(aps, bps), true)
	assert.Equal(t, g.CouldResultIn(bps, aps), false)

	// [(v3, v4), {0, [5]}] vs. [(v2, v3), {0, [5]}]
	ae = vertex.NewEdge(vs[3], vs[4])
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	be = vertex.NewEdge(vs[2], vs[3])
	bts = timestamp.NewTimestampWithParams(0, []int{5})
	bps = NewEdgePointStamp(be, bts)
	assert.Equal(t, g.CouldResultIn(aps, bps), false)
	assert.Equal(t, g.CouldResultIn(bps, aps), true)

	// [(v3, v4), {0, [5]}] vs. [(v2, v3), {0, [6]}]
	ae = vertex.NewEdge(vs[3], vs[4])
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	be = vertex.NewEdge(vs[2], vs[3])
	bts = timestamp.NewTimestampWithParams(0, []int{6})
	bps = NewEdgePointStamp(be, bts)
	assert.Equal(t, g.CouldResultIn(aps, bps), true)
	assert.Equal(t, g.CouldResultIn(bps, aps), false)

	// [(v2, v3), {0, [5]}] vs. [(v3, v3), {0, [5]}]
	ae = vertex.NewEdge(vs[2], vs[3])
	ats = timestamp.NewTimestampWithParams(0, []int{5})
	aps = NewEdgePointStamp(ae, ats)
	bts = timestamp.NewTimestampWithParams(0, []int{5})
	bps = NewVertexPointStamp(vs[3], bts)
	assert.Equal(t, g.CouldResultIn(aps, bps), true)
	assert.Equal(t, g.CouldResultIn(bps, aps), false)

	// [(v2, v3), {0, [6]}] vs. [(v3, v3), {0, [5]}]
	ae = vertex.NewEdge(vs[2], vs[3])
	ats = timestamp.NewTimestampWithParams(0, []int{6})
	aps = NewEdgePointStamp(ae, ats)
	bts = timestamp.NewTimestampWithParams(0, []int{5})
	bps = NewVertexPointStamp(vs[3], bts)
	assert.Equal(t, g.CouldResultIn(aps, bps), false)
	assert.Equal(t, g.CouldResultIn(bps, aps), true)
}

func TestIncreOCAndDecreOC(t *testing.T) {
	g := NewGraph()
	vsp := &[]vertex.Vertex{}
	BuildGraph(t, g, vsp)
	vs := *vsp

	// Just increment OC for vs[0]
	ts := timestamp.NewTimestamp()
	ps1 := NewVertexPointStamp(vs[0], ts)
	g.IncreOC(ps1)

	assert.Equal(t, len(g.activePsMap), 1)
	assert.Equal(t, g.activePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps1.Hash()].PC, 0)

	// Then increment another OC for edge[2, 3]
	ts = timestamp.NewTimestampWithParams(0, []int{5})
	e := vertex.NewEdge(vs[2], vs[3])
	ps2 := NewEdgePointStamp(e, ts)
	g.IncreOC(ps2)
	assert.Equal(t, len(g.activePsMap), 2)
	assert.Equal(t, g.activePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.activePsMap[ps2.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps2.Hash()].PC, 1)

	// Increment another OC for edge[5, 6]
	ts = timestamp.NewTimestamp()
	e = vertex.NewEdge(vs[5], vs[6])
	ps3 := NewEdgePointStamp(e, ts)
	g.IncreOC(ps3)
	assert.Equal(t, len(g.activePsMap), 3)
	assert.Equal(t, g.activePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.activePsMap[ps2.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps2.Hash()].PC, 1)
	assert.Equal(t, g.activePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps3.Hash()].PC, 2)

	// Increment yet another OC for edge[2, 3] with same pointstamp
	// Should not affect other pointstamps, just increase 1 OC
	ts = timestamp.NewTimestampWithParams(0, []int{5})
	e = vertex.NewEdge(vs[2], vs[3])
	ps4 := NewEdgePointStamp(e, ts)
	g.IncreOC(ps4)
	assert.Equal(t, len(g.activePsMap), 3)
	assert.Equal(t, g.activePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.activePsMap[ps2.Hash()].OC, 2)
	assert.Equal(t, g.activePsMap[ps2.Hash()].PC, 1)
	assert.Equal(t, g.activePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps3.Hash()].PC, 2)

	// Increment a pointstamp that is most recent
	ts = timestamp.NewTimestampWithParams(1, []int{6})
	e = vertex.NewEdge(vs[2], vs[3])
	ps5 := NewEdgePointStamp(e, ts)
	g.IncreOC(ps5)
	assert.Equal(t, len(g.activePsMap), 4)
	assert.Equal(t, g.activePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.activePsMap[ps2.Hash()].OC, 2)
	assert.Equal(t, g.activePsMap[ps2.Hash()].PC, 1)
	assert.Equal(t, g.activePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps3.Hash()].PC, 2)
	assert.Equal(t, g.activePsMap[ps5.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps5.Hash()].PC, 2)

	// Decrement ps4 a.k.a. ps2
	// Should not affect other pointstamps, just decrease 1 OC
	g.DecreOC(ps4)
	assert.Equal(t, len(g.activePsMap), 4)
	assert.Equal(t, g.activePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.activePsMap[ps2.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps2.Hash()].PC, 1)
	assert.Equal(t, g.activePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps3.Hash()].PC, 2)
	assert.Equal(t, g.activePsMap[ps5.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps5.Hash()].PC, 2)

	// Decrement ps2 again
	g.DecreOC(ps2)
	assert.Equal(t, len(g.activePsMap), 3)
	_, exist := g.activePsMap[ps2.Hash()]
	assert.Equal(t, exist, false)
	assert.Equal(t, g.activePsMap[ps1.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps1.Hash()].PC, 0)
	assert.Equal(t, g.activePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps3.Hash()].PC, 1)
	assert.Equal(t, g.activePsMap[ps5.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps5.Hash()].PC, 1)

	// Decrement ps1
	g.DecreOC(ps1)
	assert.Equal(t, len(g.activePsMap), 2)
	_, exist = g.activePsMap[ps1.Hash()]
	assert.Equal(t, exist, false)
	assert.Equal(t, g.activePsMap[ps3.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps3.Hash()].PC, 0)
	assert.Equal(t, g.activePsMap[ps5.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps5.Hash()].PC, 0)

	// Decrement ps3
	g.DecreOC(ps3)
	assert.Equal(t, len(g.activePsMap), 1)
	_, exist = g.activePsMap[ps3.Hash()]
	assert.Equal(t, exist, false)
	assert.Equal(t, g.activePsMap[ps5.Hash()].OC, 1)
	assert.Equal(t, g.activePsMap[ps5.Hash()].PC, 0)

	// Decrement ps5
	g.DecreOC(ps5)
	assert.Equal(t, len(g.activePsMap), 0)
	_, exist = g.activePsMap[ps5.Hash()]
	assert.Equal(t, exist, false)
}
