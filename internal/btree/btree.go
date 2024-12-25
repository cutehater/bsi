package btree

import (
	"errors"
	"slices"
	"strings"
)

type node struct {
	keys     []string
	children []*node
}

type BTree struct {
	root     *node
	minOrder int
}

func New(minOrder int) (*BTree, error) {
	if minOrder < 2 {
		return nil, errors.New("invalid B-Tree min order")
	}
	return &BTree{
		root:     &node{},
		minOrder: minOrder,
	}, nil
}

func (t *BTree) SearchKey(key string) bool {
	currentVertex := t.root

	for currentVertex != nil {
		childIndex, ok := slices.BinarySearch(currentVertex.keys, key)
		if ok {
			return true
		} else if len(currentVertex.children) > childIndex {
			currentVertex = currentVertex.children[childIndex]
		} else {
			return false
		}
	}

	return false
}

func (t *BTree) SearchByPrefix(prefix string) []string {
	results := make([]string, 0)
	t.root.searchByPrefix(&prefix, &results)
	slices.Sort(results)
	return results
}

func (t *BTree) Insert(key string) (found bool) {
	_, _, found = t.insert(t.root, key)
	return
}

func (t *BTree) insert(v *node, key string) (newParentKey string, newParentChild *node, found bool) {
	if v == nil {
		return
	}

	index, keyFound := slices.BinarySearch(v.keys, key)
	if keyFound {
		found = true
		return
	}

	if v.isLeaf() {
		if len(v.keys) < t.minOrder*2-1 {
			v.keys = append(v.keys[:index], append([]string{key}, v.keys[index:]...)...)
		} else {
			newParentKey = v.keys[t.minOrder-1]
			newParentChild = &node{keys: append([]string{}, v.keys[t.minOrder:]...)}
			v.keys = v.keys[:t.minOrder-1]

			if key < newParentKey {
				v.keys = append(v.keys[:index], append([]string{key}, v.keys[index:]...)...)
			} else {
				index = index - t.minOrder
				newParentChild.keys = append(newParentChild.keys[:index], append([]string{key}, newParentChild.keys[index:]...)...)
			}

			if v == t.root {
				t.root = &node{
					keys:     []string{newParentKey},
					children: []*node{v, newParentChild},
				}
			}
		}
	} else {
		newKey, newChild, keyFound := t.insert(v.children[index], key)
		if newChild == nil {
			found = keyFound
			return
		}

		index, _ = slices.BinarySearch(v.keys, newKey)
		if len(v.keys) < t.minOrder*2-1 {
			v.keys = append(v.keys[:index], append([]string{newKey}, v.keys[index:]...)...)
			v.children = append(v.children[:index+1], append([]*node{newChild}, v.children[index+1:]...)...)
		} else {
			newParentKey = v.keys[t.minOrder-1]
			newParentChild = &node{
				keys:     append([]string{}, v.keys[t.minOrder:]...),
				children: append([]*node{}, v.children[t.minOrder:]...),
			}
			v.keys = v.keys[:t.minOrder-1]
			v.children = v.children[:t.minOrder]

			if newKey < newParentKey {
				v.keys = append(v.keys[:index], append([]string{newKey}, v.keys[index:]...)...)
				v.children = append(v.children[:index+1], append([]*node{newChild}, v.children[index+1:]...)...)
			} else {
				index = index - t.minOrder
				newParentChild.keys = append(newParentChild.keys[:index], append([]string{newKey}, newParentChild.keys[index:]...)...)
				newParentChild.children = append(newParentChild.children[:index+1], append([]*node{newChild}, newParentChild.children[index+1:]...)...)
			}

			if v == t.root {
				t.root = &node{
					keys:     []string{newParentKey},
					children: []*node{v, newParentChild},
				}
			}
		}
	}

	return
}

func (v *node) searchByPrefix(prefix *string, results *[]string) {
	if v == nil {
		return
	}

	leftIndex, keyFound := slices.BinarySearch(v.keys, *prefix)
	rightIndex := leftIndex
	for ; rightIndex < len(v.keys) && strings.HasPrefix(v.keys[rightIndex], *prefix); rightIndex++ {
	}
	*results = append(*results, v.keys[leftIndex:rightIndex]...)

	if !v.isLeaf() {
		if keyFound {
			leftIndex++
		}
		for ; leftIndex <= rightIndex && leftIndex < len(v.children); leftIndex++ {
			v.children[leftIndex].searchByPrefix(prefix, results)
		}
	}
}

func (v *node) isLeaf() bool {
	return v != nil && len(v.children) == 0
}
