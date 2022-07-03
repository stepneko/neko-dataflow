package scheduler

import (
	"errors"

	"github.com/stepneko/neko-dataflow/vertex"
)

type Node struct {
	vertex   vertex.Vertex
	children map[*Node]bool
}

func NewNode(v vertex.Vertex) *Node {
	return &Node{
		vertex:   v,
		children: make(map[*Node]bool),
	}
}

type PointstampCounter struct {
	OC int
	PC int
}

type Graph struct {
	// A quick look up table to find Nodes in the graph
	// given input vertex
	vertexMap map[vertex.Vertex]*Node
	// Occurrence counts
	activePsMap map[vertex.Pointstamp]*PointstampCounter
}

func NewGraph() *Graph {
	return &Graph{
		vertexMap:   make(map[vertex.Vertex]*Node),
		activePsMap: make(map[vertex.Pointstamp]*PointstampCounter),
	}
}

func (g *Graph) InsertVertex(v vertex.Vertex) {
	_, exist := g.vertexMap[v]
	if !exist {
		node := NewNode(v)
		g.vertexMap[v] = node
	}
}

func (g *Graph) InsertEdge(e vertex.Edge) {
	src := e.GetSrc()
	srcNode, exist := g.vertexMap[src]
	if !exist {
		srcNode = NewNode(src)
		g.vertexMap[src] = srcNode
	}

	target := e.GetTarget()
	targetNode, exist := g.vertexMap[target]
	if !exist {
		targetNode = NewNode(target)
		g.vertexMap[target] = targetNode
	}

	srcNode.children[targetNode] = true
}

// When a pointstamp p becomes active, the scheduler initializes its precursor count
// to the number of existing active pointstamps that could-result- in p.
// At the same time, the scheduler increments the precursor count of any pointstamp that p could-result-in.
func (g *Graph) IncreOC(ps vertex.Pointstamp) error {
	if _, exist := g.activePsMap[ps]; !exist {
		psCounter := &PointstampCounter{
			OC: 0,
			PC: 0,
		}
		for currPs := range g.activePsMap {
			if g.CouldResultIn(currPs, ps) {
				psCounter.PC += 1
			}
		}
		for currPs := range g.activePsMap {
			if g.CouldResultIn(ps, currPs) {
				g.activePsMap[currPs].PC += 1
			}
		}
		g.activePsMap[ps] = psCounter
	}
	g.activePsMap[ps].OC += 1
	return nil
}

// A pointstamp p leaves the active set when its occurrence count drops to zero,
// at which point the scheduler decrements the precursor count for any pointstamp that p could-result-in.
// When an active pointstamp p’s precursor count is zero, there is no other pointstamp in the active set
// that could-result-in p, and we say that p is in the frontier of active pointstamps.
// The scheduler may deliver any notification in the frontier.
func (g *Graph) DecreOC(ps vertex.Pointstamp) error {
	_, exist := g.activePsMap[ps]
	if !exist {
		return errors.New("trying to decre a pointstamp which does not exist in active pointstamp map")
	}
	g.activePsMap[ps].OC -= 1
	if g.activePsMap[ps].OC == 0 {
		delete(g.activePsMap, ps)
		for currPs := range g.activePsMap {
			if g.CouldResultIn(ps, currPs) {
				g.activePsMap[currPs].PC -= 1
			}
		}
	}
	return nil
}

// CouldResultIn traverses the graph and computes whether pointstamp a could result in pointstamp b
func (g *Graph) CouldResultIn(a vertex.Pointstamp, b vertex.Pointstamp) bool {
	// Traverse from pointstamp a until it reaches pointstamp b
	// Therefore we pick target of a as starting point, until it reaches src of b
	visited := make(map[vertex.Vertex]*vertex.Timestamp)
	srcVertex := a.GetTarget()
	// The srcTs is the timestamp getting out of the a edge.
	// Therefore it has to be processed by target vertex of that edge.
	srcTs := a.GetTimestamp()
	srcVertex.HandleTimestamp(srcTs)
	targetVertex := b.GetSrc()
	targetTs := b.GetTimestamp()
	queue := []*vertex.VertexPointStamp{vertex.NewVertexPointStamp(srcVertex, srcTs)}

	// Start BFS traversal
	for len(queue) != 0 {
		currPs := queue[0]
		queue = queue[1:]

		currVertex := currPs.GetSrc()
		currTs := currPs.GetTimestamp()

		if currVertex == targetVertex &&
			(vertex.LessThan(currTs, targetTs) || vertex.Equal(currTs, targetTs)) {
			return true
		}

		if prevTs, exist := visited[currVertex]; exist {
			// If we already reached this currVertex with a smaller timestamp,
			// then just stop traversing from this current later timestamp.
			if vertex.LessThan(prevTs, currTs) || vertex.Equal(prevTs, currTs) {
				continue
			}
		}
		visited[currVertex] = currTs

		// Now go to node to traverse graph
		currNode := g.vertexMap[currVertex]
		for childNode := range currNode.children {
			childVertex := childNode.vertex
			// Handle timestamp according to the child vertex
			newTs := vertex.CopyTimestampFrom(currTs)
			childVertex.HandleTimestamp(newTs)
			newPs := vertex.NewVertexPointStamp(childVertex, newTs)
			queue = append(queue, newPs)
		}
	}
	return false
}
