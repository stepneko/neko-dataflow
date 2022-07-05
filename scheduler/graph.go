package scheduler

import (
	"errors"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/timestamp"
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

type Graph struct {
	// A quick look up table to find Nodes in the graph
	// given input vertex
	vertexMap map[vertex.Vertex]*Node
	// Occurrence counts and precursor counts map for pointstamps.
	// Key should be pointstamp, but since pointstamp is a struct
	// and is not natively hashable, we use string as key, which is the
	// digest hash of pointstamp.
	activePsMap map[string]*PointstampCounter
}

func NewGraph() *Graph {
	return &Graph{
		vertexMap:   make(map[vertex.Vertex]*Node),
		activePsMap: make(map[string]*PointstampCounter),
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
func (g *Graph) IncreOC(ps Pointstamp) error {
	psHash := ps.Hash()
	if _, exist := g.activePsMap[psHash]; !exist {
		psCounter := &PointstampCounter{
			PS: ps,
			OC: 0,
			PC: 0,
		}
		for currPsHash := range g.activePsMap {
			currPs := g.activePsMap[currPsHash].PS
			if g.CouldResultIn(currPs, ps) {
				psCounter.PC += 1
			}
		}
		for currPsHash := range g.activePsMap {
			currPs := g.activePsMap[currPsHash].PS
			if g.CouldResultIn(ps, currPs) {
				g.activePsMap[currPsHash].PC += 1
			}
		}
		g.activePsMap[psHash] = psCounter
	}
	g.activePsMap[psHash].OC += 1
	return nil
}

// A pointstamp p leaves the active set when its occurrence count drops to zero,
// at which point the scheduler decrements the precursor count for any pointstamp that p could-result-in.
// When an active pointstamp p’s precursor count is zero, there is no other pointstamp in the active set
// that could-result-in p, and we say that p is in the frontier of active pointstamps.
// The scheduler may deliver any notification in the frontier.
func (g *Graph) DecreOC(ps Pointstamp) error {
	psHash := ps.Hash()
	_, exist := g.activePsMap[psHash]
	if !exist {
		return errors.New("trying to decre a pointstamp which does not exist in active pointstamp map")
	}
	g.activePsMap[psHash].OC -= 1
	if g.activePsMap[psHash].OC == 0 {
		delete(g.activePsMap, psHash)
		for currPsHash := range g.activePsMap {
			currPs := g.activePsMap[currPsHash].PS
			if g.CouldResultIn(ps, currPs) {
				g.activePsMap[currPsHash].PC -= 1
			}
		}
	}
	return nil
}

// CouldResultIn traverses the graph and computes whether pointstamp a could result in pointstamp b
func (g *Graph) CouldResultIn(a Pointstamp, b Pointstamp) bool {
	// Traverse from pointstamp a until it reaches pointstamp b
	// Therefore we pick target of a as starting point, until it reaches src of b
	visited := make(map[vertex.Vertex]*timestamp.Timestamp)
	srcVertex := a.GetTarget()
	// The srcTs is the timestamp getting out of the a edge.
	// Therefore it has to be processed by target vertex of that edge.
	srcTs := timestamp.CopyTimestampFrom(a.GetTimestamp())
	timestamp.HandleTimestamp(srcVertex.GetType(), srcTs)
	targetVertex := b.GetSrc()
	targetTs := b.GetTimestamp()
	queue := []*VertexPointStamp{NewVertexPointStamp(srcVertex, srcTs)}

	// Start BFS traversal
	for len(queue) != 0 {
		currPs := queue[0]
		queue = queue[1:]

		currVertex := currPs.GetSrc()
		currTs := currPs.GetTimestamp()

		if currVertex == targetVertex && timestamp.LE(currTs, targetTs) {
			return true
		}

		if prevTs, exist := visited[currVertex]; exist {
			// If we already reached this currVertex with a smaller timestamp,
			// then just stop traversing from this current later timestamp.
			if timestamp.LE(prevTs, currTs) {
				continue
			}
		}
		visited[currVertex] = currTs

		// Now go to node to traverse graph
		currNode := g.vertexMap[currVertex]
		for childNode := range currNode.children {
			childVertex := childNode.vertex
			// Handle timestamp according to the child vertex
			newTs := timestamp.CopyTimestampFrom(currTs)
			timestamp.HandleTimestamp(childVertex.GetType(), newTs)
			newPs := NewVertexPointStamp(childVertex, newTs)
			queue = append(queue, newPs)
		}
	}
	return false
}

func (g *Graph) PreProcess() {
	// Before running this graph, preprocess input vertices.
	// According to the paper:
	//
	// When a computation begins the system initializes an
	// active pointstamp at the location of each input vertex,
	// timestamped with the first epoch, with an occurrence
	// count of one and a precursor count of zero. When an
	// epoch e is marked complete the input vertex adds a new
	// active pointstamp for epoch e + 1, then removes the
	// pointstamp for e, permitting downstream notifications
	// to be delivered for epoch e. When the input vertex is
	// closed it removes any active pointstamps at its location,
	// allowing all events downstream of the input to eventually
	// drain from the computation.
	for v := range g.vertexMap {
		if v.GetType() == constants.VertexType_Input {
			ts := timestamp.NewTimestamp()
			ps := NewVertexPointStamp(v, ts)
			g.activePsMap[ps.Hash()] = &PointstampCounter{
				PS: ps,
				OC: 1,
				PC: 0,
			}
		}
	}
}
