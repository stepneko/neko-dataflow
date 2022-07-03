package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stepneko/neko-dataflow/vertex"
)

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
