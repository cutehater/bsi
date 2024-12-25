package inverted_index

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"time"

	// lemmatization "github.com/aaaton/golem/v4"
	// enDict "github.com/aaaton/golem/v4/dicts/en"
	// "github.com/bbalet/stopwords"
	"golang.org/x/example/hello/reverse"

	"inverted-index/internal/btree"
	"inverted-index/internal/lsm-tree/lsm_tree"
	roaring_bitmap "inverted-index/internal/roaring-bitmap"
)

var (
	ErrInvalidTerm              = errors.New("invalid term (stop-word?)")
	ErrUnsupportedWildcardQuery = errors.New("wildcard queries with more than one * are not supported")
)

type InvertedIndex struct {
	storage *lsm_tree.LSMTree
	// lemmatizer      *lemmatization.Lemmatizer
	documentsNumber uint16
	dict            *btree.BTree
	reverseDict     *btree.BTree
}

func New() (*InvertedIndex, error) {
	// l, err := lemmatization.New(enDict.New())
	// if err != nil {
	// 	return nil, err
	// }

	dict, err := btree.New(50)
	if err != nil {
		return nil, err
	}
	reverseDict, err := btree.New(50)
	if err != nil {
		return nil, err
	}

	return &InvertedIndex{
		storage: lsm_tree.New(),
		// lemmatizer:      l,
		documentsNumber: 0,
		dict:            dict,
		reverseDict:     reverseDict,
	}, nil
}

func (i *InvertedIndex) AddDocument(filePath string, createdTime time.Time, dieTime *time.Time) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		toAdd, processedTerm := i.processTerm(scanner.Text())
		if toAdd {
			err = i.storage.Add(processedTerm, i.documentsNumber)
			if err != nil {
				return err
			}

			i.dict.Insert(scanner.Text())
			i.reverseDict.Insert(reverse.String(scanner.Text()))
		}
	}

	createdTimeUnix := createdTime.Unix()
	dieTimeUnix := int64(math.MaxInt64)
	if dieTime != nil {
		dieTimeUnix = dieTime.Unix()
	}

	for j := 0; createdTimeUnix > 0 || dieTimeUnix > 0; j++ {
		if createdTimeUnix&1 == 1 {
			err = i.storage.Add(uint16(j), i.documentsNumber)
			if err != nil {
				return err
			}
		}
		createdTimeUnix >>= 1

		if dieTimeUnix&1 == 1 {
			err = i.storage.Add(uint16(j+64), i.documentsNumber)
			if err != nil {
				return err
			}
		}
		dieTimeUnix >>= 1
	}

	i.documentsNumber++
	return scanner.Err()
}

func (i *InvertedIndex) ConvertFromContainer(c roaring_bitmap.Container) []int {
	result := make([]int, 0)

	switch c.(type) {
	case *roaring_bitmap.Array:
		for _, v := range c.(*roaring_bitmap.Array).Values {
			result = append(result, int(v))
		}
	case *roaring_bitmap.Bitmap:
		b := c.(*roaring_bitmap.Bitmap).Values
		for idx, e := b.NextSet(0); e; idx, e = b.NextSet(idx + 1) {
			result = append(result, int(idx))
		}
	case *roaring_bitmap.Run:
		for _, v := range c.(*roaring_bitmap.Run).Values {
			for idx := 0; idx <= int(v.Length); idx++ {
				result = append(result, int(v.Start)+idx)
			}
		}
	}

	for l, r := 0, len(result)-1; l < r; l, r = l+1, r-1 {
		result[l], result[r] = result[r], result[l]
	}
	return result
}

func (i *InvertedIndex) processTerm(term string) (toAdd bool, processedFeature uint16) {
	// term = strings.TrimSpace(stopwords.CleanString(term, "en", false))
	// if len(term) == 0 {
	// 	 return false, 0
	// }

	// lemma := i.lemmatizer.LemmaLower(term)

	for {
		hash := sha256.Sum256([]byte(term))
		value := binary.BigEndian.Uint16(hash[:2])
		if value >= 128 {
			return true, value
		}
		// Rehashing for no intersection with date features
		term += "salt"
	}
}
