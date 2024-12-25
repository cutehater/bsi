package inverted_index

import (
	"golang.org/x/example/hello/reverse"
	roaring_bitmap "inverted-index/internal/roaring-bitmap"
	"slices"
	"strings"
)

func (i *InvertedIndex) PreciseQuery(query string) (roaring_bitmap.Container, error) {
	if ok, processedTerm := i.processTerm(query); !ok {
		return nil, ErrInvalidTerm
	} else {
		return i.storage.Search(processedTerm)
	}
}

func (i *InvertedIndex) WildcardQuery(query string) (roaring_bitmap.Container, error) {
	queryParts := strings.Split(query, "*")
	if len(queryParts) == 1 {
		return i.PreciseQuery(query)
	} else if len(queryParts) > 2 {
		return nil, ErrUnsupportedWildcardQuery
	}

	prefixQuery := []string(nil)
	suffixQuery := []string(nil)
	if len(queryParts[0]) > 0 {
		prefixQuery = i.dict.SearchByPrefix(queryParts[0])
	}
	if len(queryParts[1]) > 0 {
		suffixQuery = i.reverseDict.SearchByPrefix(reverse.String(queryParts[1]))
	}
	for j := range suffixQuery {
		suffixQuery[j] = reverse.String(suffixQuery[j])
	}

	resultTerms := make([]string, 0, len(prefixQuery))
	slices.Sort(prefixQuery)
	slices.Sort(suffixQuery)

	if prefixQuery == nil {
		resultTerms = suffixQuery
	} else if suffixQuery == nil {
		resultTerms = prefixQuery
	} else {
		prefixIdx, suffixIdx := 0, 0
		for prefixIdx < len(prefixQuery) && suffixIdx < len(suffixQuery) {
			if prefixQuery[prefixIdx] == suffixQuery[suffixIdx] {
				resultTerms = append(resultTerms, prefixQuery[prefixIdx])
				prefixIdx++
				suffixIdx++
			} else if prefixQuery[prefixIdx] < suffixQuery[suffixIdx] {
				prefixIdx++
			} else {
				suffixIdx++
			}
		}
	}

	var resultContainer roaring_bitmap.Container
	for _, term := range resultTerms {
		c, err := i.PreciseQuery(term)
		if err != nil {
			return nil, err
		}
		resultContainer = i.Or(resultContainer, c)
	}

	return resultContainer, nil
}
