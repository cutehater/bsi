package btree

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	minLatinSymbol = 97
	rangeASCII     = 26
	maxRandLength  = 15
)

func randString() string {
	length := rand.Intn(maxRandLength) + 8
	b := make([]byte, length)
	for i := range b {
		b[i] = byte(rand.Intn(rangeASCII) + minLatinSymbol)
	}
	return string(b)
}

func printTree(v *node, lay int, cnt *int) {
	fmt.Printf("%d: lay %d keys %v\n", *cnt, lay, v.keys)
	*cnt++

	for _, u := range v.children {
		printTree(u, lay+1, cnt)
	}
}

func TestBTree_SearchKey(t *testing.T) {
	tree, err := New(4)
	require.NoError(t, err)

	insertedStrings := make([]string, 0, 10000)
	for i := 0; i < 10000; i++ {
		s := randString()
		insertedStrings = append(insertedStrings, s)
		ok := tree.Insert(s)
		require.False(t, ok)
	}

	for _, s := range insertedStrings {
		ok := tree.SearchKey(s)
		require.True(t, ok)
	}
	for i := 0; i < 100; i++ {
		require.False(t, tree.SearchKey(randString()))
	}
}

func TestBTree_SearchKey_BigOrder(t *testing.T) {
	tree, err := New(100)
	require.NoError(t, err)

	insertedStrings := make([]string, 0, 1000000)
	for i := 0; i < 1000000; i++ {
		s := randString()
		insertedStrings = append(insertedStrings, s)
		ok := tree.Insert(s)
		require.False(t, ok)
	}

	for _, s := range insertedStrings {
		ok := tree.SearchKey(s)
		require.True(t, ok)
	}
	for i := 0; i < 100; i++ {
		require.False(t, tree.SearchKey(randString()))
	}
}

func TestBTree_SearchByPrefix(t *testing.T) {
	tree, err := New(4)
	require.NoError(t, err)

	insertedStrings := make([]string, 0, 10000)
	for i := 0; i < 10000; i++ {
		s := randString()
		insertedStrings = append(insertedStrings, s)
		ok := tree.Insert(s)
		require.False(t, ok)
	}

	for c := uint8('a'); c <= 'z'; c++ {
		expected := make([]string, 0)
		for _, s := range insertedStrings {
			if s[0] == c {
				expected = append(expected, s)
			}
		}
		slices.Sort(expected)

		result := tree.SearchByPrefix(string(c))
		slices.Sort(result)

		require.Equal(t, expected, result)
	}

}
