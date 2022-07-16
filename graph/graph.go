package graph

import (
	"errors"
	"fmt"

	"github.com/stepneko/neko-dataflow/constants"
	"github.com/stepneko/neko-dataflow/edge"
	"github.com/stepneko/neko-dataflow/timestamp"
)

type Node struct {
	Vid      constants.VertexId
	Typ      constants.VertexType
	Children map[*Node]bool
}

func NewNode(vid constants.VertexId, typ constants.VertexType) *Node {
	return &Node{
		Vid:      vid,
		Typ:      typ,
		Children: make(map[*Node]bool),
	}
}

type Graph struct {
	// A quick look up table to find Nodes in the graph
	// given input vertex
	VertexMap map[constants.VertexId]*Node
	// Map recording if an edge from src vertex A to target vertex B is
	// pointing to the left or right of the target B.
	DirsMap map[constants.VertexId]map[constants.VertexId]constants.VertexInDir
	// Occurrence counts and precursor counts map for pointstamps.
	// Key should be pointstamp, but since pointstamp is a struct
	// and is not natively hashable, we use string as key, which is the
	// digest hash of pointstamp.
	ActivePsMap map[string]*PointstampCounter
}

func NewGraph() *Graph {
	return &Graph{
		VertexMap:   make(map[constants.VertexId]*Node),
		DirsMap:     make(map[constants.VertexId]map[constants.VertexId]constants.VertexInDir),
		ActivePsMap: make(map[string]*PointstampCounter),
	}
}

func (g *Graph) InsertVertex(vid constants.VertexId, typ constants.VertexType) {
	_, exist := g.VertexMap[vid]
	if !exist {
		node := NewNode(vid, typ)
		g.VertexMap[vid] = node
	}
}

func (g *Graph) InsertEdge(e edge.Edge) error {
	src := e.GetSrc()
	srcNode, exist := g.VertexMap[src]
	if !exist {
		return errors.New(fmt.Sprintf("src vertex not registered with id: %d", src))
	}

	target := e.GetTarget()
	targetNode, exist := g.VertexMap[target]
	if !exist {
		return errors.New(fmt.Sprintf("target vertex not registered with id: %d", target))
	}

	srcNode.Children[targetNode] = true

	return nil
}

func (g *Graph) GetDir(src constants.VertexId, target constants.VertexId) (constants.VertexInDir, error) {
	m, exist := g.DirsMap[src]
	if !exist {
		return constants.VertexInDir_Default, errors.New("src not found in DirMap in graph")
	}
	dir, exist := m[target]
	if !exist {
		return constants.VertexInDir_Default, errors.New("target not found in DirMap in geraph")
	}
	return dir, nil
}

// When a pointstamp p becomes active, the scheduler initializes its precursor count
// to the number of existing active pointstamps that could-result- in p.
// At the same time, the scheduler increments the precursor count of any pointstamp that p could-result-in.
func (g *Graph) IncreOC(ps Pointstamp) error {
	psHash := ps.Hash()
	if _, exist := g.ActivePsMap[psHash]; !exist {
		psCounter := &PointstampCounter{
			PS: ps,
			OC: 0,
			PC: 0,
		}
		for currPsHash := range g.ActivePsMap {
			currPs := g.ActivePsMap[currPsHash].PS
			res, err := g.CouldResultIn(currPs, ps)
			if err != nil {
				return err
			}
			if res {
				psCounter.PC += 1
			}
		}
		for currPsHash := range g.ActivePsMap {
			currPs := g.ActivePsMap[currPsHash].PS
			res, err := g.CouldResultIn(ps, currPs)
			if err != nil {
				return err
			}
			if res {
				g.ActivePsMap[currPsHash].PC += 1
			}
		}
		g.ActivePsMap[psHash] = psCounter
	}
	g.ActivePsMap[psHash].OC += 1
	return nil
}

// A pointstamp p leaves the active set when its occurrence count drops to zero,
// at which point the scheduler decrements the precursor count for any pointstamp that p could-result-in.
// When an active pointstamp p’s precursor count is zero, there is no other pointstamp in the active set
// that could-result-in p, and we say that p is in the frontier of active pointstamps.
// The scheduler may deliver any notification in the frontier.
func (g *Graph) DecreOC(ps Pointstamp) error {
	psHash := ps.Hash()
	_, exist := g.ActivePsMap[psHash]
	if !exist {
		return errors.New("trying to decre a pointstamp which does not exist in active pointstamp map")
	}
	g.ActivePsMap[psHash].OC -= 1
	if g.ActivePsMap[psHash].OC == 0 {
		delete(g.ActivePsMap, psHash)
		for currPsHash := range g.ActivePsMap {
			currPs := g.ActivePsMap[currPsHash].PS
			res, err := g.CouldResultIn(ps, currPs)
			if err != nil {
				return err
			}
			if res {
				g.ActivePsMap[currPsHash].PC -= 1
			}
		}
	}
	return nil
}

// CouldResultIn traverses the graph and computes whether pointstamp a could result in pointstamp b
func (g *Graph) CouldResultIn(a Pointstamp, b Pointstamp) (bool, error) {
	// Traverse from pointstamp a until it reaches pointstamp b
	// Therefore we pick target of a as starting point, until it reaches src of b
	visited := make(map[constants.VertexId]*timestamp.Timestamp)
	srcId := a.GetTarget()
	srcNode, exist := g.VertexMap[srcId]
	if !exist {
		return false, errors.New(fmt.Sprintf("src not registered with id: %d", srcId))
	}
	// The srcTs is the timestamp getting out of the a edge.
	// Therefore it has to be processed by target vertex of that edge.
	srcTs := timestamp.CopyTimestampFrom(a.GetTimestamp())
	timestamp.HandleTimestamp(srcNode.Typ, srcTs)
	targetId := b.GetSrc()
	_, exist = g.VertexMap[targetId]
	if !exist {
		return false, errors.New(fmt.Sprintf("target not registered with id: %d", targetId))
	}
	targetTs := b.GetTimestamp()
	queue := []*VertexPointStamp{NewVertexPointStamp(srcId, srcTs)}

	// Start BFS traversal
	for len(queue) != 0 {
		currPs := queue[0]
		queue = queue[1:]

		currId := currPs.GetSrc()
		currNode, exist := g.VertexMap[currId]
		if !exist {
			return false, errors.New(fmt.Sprintf("vertex not registered with id: %d", currId))
		}
		currTs := currPs.GetTimestamp()

		if currId == targetId && timestamp.LE(currTs, targetTs) {
			return true, nil
		}

		if prevTs, exist := visited[currId]; exist {
			// If we already reached this currVertex with a smaller timestamp,
			// then just stop traversing from this current later timestamp.
			if timestamp.LE(prevTs, currTs) {
				continue
			}
		}
		visited[currId] = currTs

		// Now go to node to traverse graph
		for childNode := range currNode.Children {
			// Handle timestamp according to the child vertex
			newTs := timestamp.CopyTimestampFrom(currTs)
			timestamp.HandleTimestamp(childNode.Typ, newTs)
			newPs := NewVertexPointStamp(childNode.Vid, newTs)
			queue = append(queue, newPs)
		}
	}
	return false, nil
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
	for vertexId := range g.VertexMap {
		vNode := g.VertexMap[vertexId]
		if vNode.Typ == constants.VertexType_Input {
			ts := timestamp.NewTimestamp()
			ps := NewVertexPointStamp(vertexId, ts)
			g.ActivePsMap[ps.Hash()] = &PointstampCounter{
				PS: ps,
				OC: 1,
				PC: 0,
			}
		}
	}
}
