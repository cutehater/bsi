package test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	inverted_index "inverted-index/internal/inverted-index"
)

func TestSimple(t *testing.T) {
	invertedIndex, err := inverted_index.New()
	require.NoError(t, err)

	err = invertedIndex.AddDocument("./shakespeare.txt", time.Now(), nil)
	require.NoError(t, err)

	docIDsContainer, err := invertedIndex.PreciseQuery("diamond")
	require.NoError(t, err)

	docIDs := invertedIndex.ConvertFromContainer(docIDsContainer)
	require.Len(t, docIDs, 1)
	require.Equal(t, 0, docIDs[0])
}

func TestOr(t *testing.T) {
	invertedIndex, err := inverted_index.New()
	require.NoError(t, err)

	err = invertedIndex.AddDocument("./shakespeare.txt", time.Now(), nil)
	require.NoError(t, err)
	err = invertedIndex.AddDocument("./some_words.txt", time.Now(), nil)
	require.NoError(t, err)

	docIDsContainer1, err := invertedIndex.PreciseQuery("rose")
	require.NoError(t, err)
	docIDsContainer2, err := invertedIndex.PreciseQuery("space")
	require.NoError(t, err)
	orContainers := invertedIndex.Or(docIDsContainer1, docIDsContainer2)

	docIDs := invertedIndex.ConvertFromContainer(orContainers)
	require.Len(t, docIDs, 2)
	require.Equal(t, 1, docIDs[0])
	require.Equal(t, 0, docIDs[1])
}

func TestAnd(t *testing.T) {
	invertedIndex, err := inverted_index.New()
	require.NoError(t, err)

	err = invertedIndex.AddDocument("./shakespeare.txt", time.Now(), nil)
	require.NoError(t, err)
	err = invertedIndex.AddDocument("./some_words.txt", time.Now(), nil)
	require.NoError(t, err)

	docIDsContainer1, err := invertedIndex.PreciseQuery("diamond")
	require.NoError(t, err)
	docIDsContainer2, err := invertedIndex.PreciseQuery("space")
	require.NoError(t, err)
	andContainers := invertedIndex.And(docIDsContainer1, docIDsContainer2)

	docIDs := invertedIndex.ConvertFromContainer(andContainers)
	require.Len(t, docIDs, 1)
	require.Equal(t, 0, docIDs[0])
}

func TestNot(t *testing.T) {
	invertedIndex, err := inverted_index.New()
	require.NoError(t, err)

	err = invertedIndex.AddDocument("./shakespeare.txt", time.Now(), nil)
	require.NoError(t, err)
	err = invertedIndex.AddDocument("./some_words.txt", time.Now(), nil)
	require.NoError(t, err)

	docIDsContainer1, err := invertedIndex.PreciseQuery("diamond")
	require.NoError(t, err)
	docIDsContainer2, err := invertedIndex.PreciseQuery("space")
	require.NoError(t, err)
	orContainers := invertedIndex.And(docIDsContainer1, docIDsContainer2)
	notContainers := invertedIndex.Not(orContainers)

	docIDs := invertedIndex.ConvertFromContainer(notContainers)
	require.Len(t, docIDs, 1)
	require.Equal(t, 1, docIDs[0])
}

func TestWildcard(t *testing.T) {
	invertedIndex, err := inverted_index.New()
	require.NoError(t, err)

	err = invertedIndex.AddDocument("./shakespeare.txt", time.Now(), nil)
	require.NoError(t, err)
	err = invertedIndex.AddDocument("./some_words.txt", time.Now(), nil)
	require.NoError(t, err)
	err = invertedIndex.AddDocument("./disturbia.txt", time.Now(), nil)
	require.NoError(t, err)

	docIDsContainer, err := invertedIndex.WildcardQuery("di*")
	require.NoError(t, err)
	docIDs := invertedIndex.ConvertFromContainer(docIDsContainer)
	require.Len(t, docIDs, 3)

	docIDsContainer, err = invertedIndex.WildcardQuery("dia*")
	require.NoError(t, err)
	docIDs = invertedIndex.ConvertFromContainer(docIDsContainer)
	require.Len(t, docIDs, 2)
	require.Equal(t, 1, docIDs[0])
	require.Equal(t, 0, docIDs[1])

	docIDsContainer, err = invertedIndex.WildcardQuery("di*d")
	require.NoError(t, err)
	docIDs = invertedIndex.ConvertFromContainer(docIDsContainer)
	require.Len(t, docIDs, 2)
	require.Equal(t, 1, docIDs[0])
	require.Equal(t, 0, docIDs[1])

	docIDsContainer, err = invertedIndex.WildcardQuery("di*a")
	require.NoError(t, err)
	docIDs = invertedIndex.ConvertFromContainer(docIDsContainer)
	require.Len(t, docIDs, 1)
	require.Equal(t, 2, docIDs[0])

	docIDsContainer, err = invertedIndex.WildcardQuery("cre*es")
	require.NoError(t, err)
	docIDs = invertedIndex.ConvertFromContainer(docIDsContainer)
	require.Len(t, docIDs, 1)
	require.Equal(t, 0, docIDs[0])
}

func TestDateQuery(t *testing.T) {
	invertedIndex, err := inverted_index.New()
	require.NoError(t, err)

	err = invertedIndex.AddDocument(
		"./shakespeare.txt",
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		nil,
	)
	require.NoError(t, err)
	date := time.Date(2015, time.April, 8, 4, 20, 0, 0, time.UTC)
	err = invertedIndex.AddDocument(
		"./some_words.txt",
		time.Date(2014, time.April, 8, 4, 20, 0, 0, time.UTC),
		&date,
	)
	require.NoError(t, err)
	err = invertedIndex.AddDocument(
		"./disturbia.txt",
		time.Date(2020, time.December, 14, 23, 30, 15, 0, time.UTC),
		nil,
	)
	require.NoError(t, err)

	// simple CREATED IN RANGE query (single date range)
	docIDsContainer, err := invertedIndex.DateQueryCreated(
		time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)
	docIDs := invertedIndex.ConvertFromContainer(docIDsContainer)
	require.Len(t, docIDs, 2)
	require.Equal(t, 1, docIDs[0])
	require.Equal(t, 0, docIDs[1])

	// more complex CREATED IN RANGE query (boolean expression of date ranges)
	docIDsContainer2, err := invertedIndex.DateQueryCreated(
		time.Date(2020, time.December, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, time.December, 31, 23, 59, 59, 0, time.UTC),
	)
	require.NoError(t, err)
	orContainers := invertedIndex.Or(docIDsContainer, docIDsContainer2)
	docIDs = invertedIndex.ConvertFromContainer(orContainers)
	require.Len(t, docIDs, 3)
	require.Equal(t, []int{2, 1, 0}, docIDs)

	// VALID IN RANGE query
	docIDsContainer, err = invertedIndex.DateQueryValid(
		time.Date(2010, time.December, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, time.December, 1, 23, 59, 59, 0, time.UTC),
	)
	require.NoError(t, err)
	docIDs = invertedIndex.ConvertFromContainer(docIDsContainer)
	require.Len(t, docIDs, 1)
	require.Equal(t, 0, docIDs[0])
}
