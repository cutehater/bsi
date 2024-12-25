package sstable

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"inverted-index/internal/lsm-tree/bloom_filter"
	"inverted-index/internal/lsm-tree/common"
	roaring_bitmap "inverted-index/internal/roaring-bitmap"
)

type SearchResult int

type SSTable struct {
	metaFile    *os.File
	dataFile    *os.File
	cookie      *cookieData
	size        int
	bloomFilter bloom_filter.BloomFilter
}

func New(metaFilepath string, dataFilepath string, tablesToMerge []*SSTable) (*SSTable, error) {
	if len(tablesToMerge) != common.MaxLevelSize {
		return nil, fmt.Errorf("number of tables to merge is not equal to level size")
	}

	sizeEstimation := 0
	for _, table := range tablesToMerge {
		sizeEstimation += table.size
	}

	s := &SSTable{bloomFilter: bloom_filter.New(sizeEstimation)}

	var err error
	s.metaFile, err = createFile(metaFilepath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFileCreating, err)
	}

	s.dataFile, err = createFile(dataFilepath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFileCreating, err)
	}

	err = s.merge(tablesToMerge)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMergingTables, err)
	}

	return s, nil
}

func NewFromMap(metaFilepath string, dataFilepath string, valuesToAdd map[uint16]roaring_bitmap.Container) (*SSTable, error) {
	s := &SSTable{bloomFilter: bloom_filter.New(common.FirstLevelSize)}

	var err error

	s.metaFile, err = createFile(metaFilepath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFileCreating, err)
	}
	metaWriter := bufio.NewWriter(s.metaFile)
	defer metaWriter.Flush()

	s.dataFile, err = createFile(dataFilepath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFileCreating, err)
	}
	dataWriter := bufio.NewWriter(s.dataFile)
	defer dataWriter.Flush()

	valuesSorted := make([]TableElement, len(valuesToAdd))
	i := 0
	for key, value := range valuesToAdd {
		valuesSorted[i] = TableElement{
			Key:   key,
			Value: value,
		}
		i++
	}
	sort.Slice(valuesSorted, func(i, j int) bool {
		return valuesSorted[i].Key < valuesSorted[j].Key
	})

	offset := 0
	for _, value := range valuesSorted {
		err = s.writeElement(metaWriter, dataWriter, &value, &offset)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *SSTable) SearchKey(key uint16) (*TableElement, error) {
	keyBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(keyBytes[:], key)
	if ok, err := s.bloomFilter.CheckContains(keyBytes); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBloomFilter, err)
	} else if !ok {
		return nil, nil
	}

	left, right := -1, s.size
	for right-left > 1 {
		mid := (left + right) / 2
		midValue, err := tableElementFromFileRandom(s.metaFile, s.dataFile, s.cookie, int64(mid))
		if err != nil {
			return nil, err
		}
		if midValue.Key == key {
			return midValue, nil
		} else if midValue.Key < key {
			left = mid
		} else {
			right = mid
		}
	}

	return nil, nil
}

func (s *SSTable) Close() error {
	err := s.metaFile.Close()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFileClosing, err)
	}

	err = s.dataFile.Close()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFileClosing, err)
	}

	return nil
}

func (s *SSTable) Remove() error {
	err := s.Close()
	if err != nil {
		return err
	}

	err = os.Remove(s.metaFile.Name())
	if err != nil {
		return err
	}

	err = os.Remove(s.dataFile.Name())
	if err != nil {
		return err
	}

	return nil
}

func (s *SSTable) merge(tablesToMerge []*SSTable) error {
	queue := priorityQueue{}
	heap.Init(&queue)

	metaWriter := bufio.NewWriter(s.metaFile)
	defer metaWriter.Flush()
	dataWriter := bufio.NewWriter(s.dataFile)
	defer dataWriter.Flush()

	metaReaders := make([]*bufio.Reader, len(tablesToMerge))
	dataReaders := make([]*bufio.Reader, len(tablesToMerge))

	for i := 0; i < len(tablesToMerge); i++ {
		if _, err := setDataFileOffset(tablesToMerge[i].metaFile, tablesToMerge[i].dataFile, s.cookie, 0, true); err != nil {
			return err
		}
		metaReaders[i] = bufio.NewReader(tablesToMerge[i].metaFile)
		dataReaders[i] = bufio.NewReader(tablesToMerge[i].dataFile)

		element, err := tableElementFromFileConsecutive(metaReaders[i], dataReaders[i], s.cookie, 0)
		if err != nil {
			return err
		}
		heap.Push(&queue, &mergeItem{
			value:      *element,
			readerIdx:  i,
			elementIdx: 0,
		})
	}

	var toInsert *TableElement
	offset := 0
	for queue.Len() > 0 {
		element := heap.Pop(&queue).(*mergeItem)

		if offset == 0 {
			toInsert = &element.value
		} else if toInsert.Key == element.value.Key {
			toInsert.Value = roaring_bitmap.Or(toInsert.Value, element.value.Value)
		} else {
			err := s.writeElement(metaWriter, dataWriter, toInsert, &offset)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrWritingElement, err)
			}
			toInsert = &element.value
		}

		newElement, err := tableElementFromFileConsecutive(metaReaders[element.readerIdx], dataReaders[element.readerIdx], s.cookie, element.elementIdx+1)
		if err != nil && err != io.EOF {
			return err
		}
		if err != io.EOF {
			heap.Push(&queue, &mergeItem{
				value:     *newElement,
				readerIdx: element.readerIdx,
			})
		}

	}
	err := s.writeElement(metaWriter, dataWriter, toInsert, &offset)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrWritingElement, err)
	}

	return nil
}

func (s *SSTable) writeElement(metaDataWriter *bufio.Writer, dataWriter *bufio.Writer, element *TableElement, offset *int) error {
	elementBytes, err := element.toBytes()
	if err != nil {
		return err
	}
	if _, err = dataWriter.Write(elementBytes); err != nil {
		return err
	}

	elementMetaData := meta{
		key:         element.Key,
		cardinality: element.Value.GetCardinality(),
		offset:      uint32(*offset),
	}
	elementMetaDataBytes, err := elementMetaData.toBytes()
	if err != nil {
		return err
	}
	_, err = metaDataWriter.Write(elementMetaDataBytes)
	if err != nil {
		return err
	}

	keyBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(keyBytes[:], element.Key)
	err = s.bloomFilter.Add(keyBytes)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBloomFilter, err)
	}

	switch element.Value.(type) {
	case *roaring_bitmap.Run:
		s.cookie.runFlagBitset.Set(uint(s.size))
	}
	s.size++
	*offset += len(elementBytes)

	return nil
}

func createFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0770); err != nil {
		return nil, err
	}
	return os.Create(path)
}
