package skiplist

import (
	"bytes"
	"math/rand"
)

//NodeID rapresent a pointer
type NodeID int

//Node is the main type contained inside the StringSk
type Node struct {
	Next  []NodeID
	Value []byte
}

// returns the height of the StringSk
func (n *Node) height() int {
	h := len(n.Next)
	if h == 0 {
		return 0
	}

	return h - 1
}

func (n *Node) isNotNull(a *Arena, i int) bool {
	if id := n.Next[i]; id == 0 {
		return false
	}

	value := a.ValueFromID(n.Next[i])
	if size := len(value); size != 0 {
		return true
	}

	return false
}

func newNode(height int) *Node {
	return &Node{Next: make([]NodeID, height)}
}

// SkipList is the SkipList data structure
type SkipList struct {
	arena     *Arena
	stack     []*Node
	sentinel  *Node
	nodeCount uint
}

//New creates new SkipList
func New() *SkipList {
	sk := &SkipList{
		arena:     newArena(),
		stack:     make([]*Node, 128),
		sentinel:  nil,
		nodeCount: 0,
	}

	sk.sentinel = sk.arena.NodeFromID(sk.arena.allocate([]byte{}, 1))

	return sk
}

// Size returns the nodeCount of Nodes in the StringSk
// except for the sentinel Node
func (s *SkipList) Size() uint { return s.nodeCount }

// Height returns the current height of the StringSk
func (s *SkipList) Height() int { return len(s.sentinel.Next) - 1 }

func (s *SkipList) findPrev(v []byte) *Node {
	n := s.sentinel
	h := n.height()
	a := s.arena

	for ; h >= 0; h-- {
		for n.isNotNull(a, h) && bytes.Compare(a.ValueFromID(n.Next[h]), v) < 0 {
			n = a.NodeFromID(n.Next[h])
		}
	}

	return n
}

// Find tries to look for a value and returns the tuple value, boolean
// true if the value was found false if it wasnt' found
func (s *SkipList) Find(v []byte) bool {
	n := s.findPrev(v)
	a := s.arena

	if n.isNotNull(a, 0) && bytes.Equal(a.ValueFromID(n.Next[0]), v) {
		return true
	}

	return false
}

// RangeFind does a range query from start element till end element returns
// success or failure in form of a boolean and a the list of found values
// fails optmistiacally meaning if the start value is not found the query
// does not start at all, if the end value is not found the query run till
// a bigger value is found or there are no more elements on the list
func (s SkipList) RangeFind(start []byte, end []byte) (ok bool, found [][]byte) {
	n := s.findPrev(start)
	a := s.arena

	if n.isNotNull(a, 0) && bytes.Equal(a.ValueFromID(n.Next[0]), start) {
		for ; n.isNotNull(a, 0); n = a.NodeFromID(n.Next[0]) {
			value := a.ValueFromID(n.Next[0])
			found = append(found, value)

			if bytes.Equal(value, end) {
				return true, found
			}

			//if bytes.Compare(value, end) == 0 {
			//	return false, found
			//}
		}
	}

	return ok, found
}

func (s *SkipList) pickHeight() int {
	var (
		k int
		m = 1
	)

	z := rand.Intn(39751)
	for (z & m) != 0 {
		k++
		m <<= 1
	}

	return int(k) + 1
}

// Insert a new value and returns true or false based on success or failure
func (s *SkipList) Insert(v []byte) bool {
	n := s.sentinel
	h := s.sentinel.height()
	a := s.arena

	for ; h >= 0; h-- {
		for n.isNotNull(a, h) && bytes.Compare(a.ValueFromID(n.Next[h]), v) < 0 {
			n = s.arena.NodeFromID(n.Next[h])

		}
		if n.isNotNull(a, h) && bytes.Equal(a.ValueFromID(n.Next[h]), v) {
			return false
		}
		s.stack[h] = n
	}

	newID := a.allocate(v, s.pickHeight())
	new := a.NodeFromID(newID)
	for s.sentinel.height() < new.height() {
		if len(s.stack) < new.height() {
			s.stack = append(s.stack, make([]*Node, 1)...)
		}

		s.sentinel.Next = append(s.sentinel.Next, make([]NodeID, 1)...)
		// basically increamenting stack and StringSk height
		s.stack[s.sentinel.height()] = s.sentinel
	}

	for i := 0; i < len(new.Next); i++ {
		new.Next[i] = s.stack[i].Next[i]
		s.stack[i].Next[i] = newID
	}

	s.nodeCount++

	return true
}

// Remove a new value and returns true or false based on success or failure
func (s *SkipList) Remove(v []byte) (removed bool) {
	n := s.sentinel
	h := s.sentinel.height()
	a := s.arena

	for ; h >= 0; h-- {
		for n.isNotNull(a, h) && bytes.Compare(a.ValueFromID(n.Next[h]), v) < 0 {
			n = s.arena.NodeFromID(n.Next[h])

		}
		if n.isNotNull(a, h) && bytes.Equal(a.ValueFromID(n.Next[h]), v) {
			next := a.NodeFromID(n.Next[h])
			n.Next[h] = next.Next[h]
			if n == s.sentinel && n.isNotNull(a, h) {
				s.sentinel.Next = s.sentinel.Next[:s.Height()]
			}

			removed = true
		}
	}

	if removed {
		s.nodeCount--
	}

	return removed
}
