package lsm_tree

import (
	"fmt"
	"path/filepath"
	"strconv"

	"inverted-index/internal/lsm-tree/common"
	"inverted-index/internal/lsm-tree/sstable"
	roaring_bitmap "inverted-index/internal/roaring-bitmap"
)

type LSMTree struct {
	sstables         [][]*sstable.SSTable
	ramComponent     map[uint16]roaring_bitmap.Container
	ramComponentSize int
	fileCnt          int
}

func New() *LSMTree {
	return &LSMTree{
		ramComponent: make(map[uint16]roaring_bitmap.Container),
		sstables:     make([][]*sstable.SSTable, 1),
	}
}

func (l *LSMTree) Add(key uint16, value uint16) error {
	if _, ok := l.ramComponent[key]; !ok {
		l.ramComponent[key] = &roaring_bitmap.Array{
			Cardinality: 0,
			Values:      []uint16{value},
		}
		l.ramComponentSize++
	} else {
		ok = l.ramComponent[key].Add(value)
		if ok {
			l.ramComponentSize++
		}
	}

	if len(l.ramComponent) == common.FirstLevelSize {
		err := l.flushRAMComponent()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrFlushingRAMComponent, err)
		}
	}

	return nil
}

func (l *LSMTree) Search(key uint16) (roaring_bitmap.Container, error) {
	if rb, ok := l.ramComponent[key]; ok {
		return rb, nil
	}

	for level := range len(l.sstables) {
		for i := len(l.sstables[level]) - 1; i >= 0; i-- {
			searchResult, err := l.sstables[level][i].SearchKey(key)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrSearching, err)
			}
			if searchResult != nil {
				return searchResult.Value, nil
			}
		}
	}

	return nil, nil
}

func (l *LSMTree) Clear() {
	for level := range l.sstables {
		for _, sst := range l.sstables[level] {
			_ = sst.Remove()
		}
	}
}

func (l *LSMTree) flushRAMComponent() error {
	newSSTable, err := sstable.NewFromMap(
		filepath.Join(common.MetaDataDir, strconv.Itoa(l.fileCnt)),
		filepath.Join(common.DataDir, strconv.Itoa(l.fileCnt)),
		l.ramComponent,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCreatingSSTable, err)
	}

	l.sstables[0] = append(l.sstables[0], newSSTable)
	l.fileCnt++
	l.ramComponent = make(map[uint16]roaring_bitmap.Container)
	l.ramComponentSize = 0

	err = l.mergeSSTables()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrMergingSSTables, err)
	}

	return nil
}

func (l *LSMTree) mergeSSTables() error {
	for level := 0; level < len(l.sstables); level++ {
		if len(l.sstables[level]) == common.MaxLevelSize {
			newSSTable, err := sstable.New(
				filepath.Join(common.MetaDataDir, strconv.Itoa(l.fileCnt)),
				filepath.Join(common.DataDir, strconv.Itoa(l.fileCnt)),
				l.sstables[level],
			)
			if err != nil {
				return err
			}

			for _, table := range l.sstables[level] {
				err = table.Remove()
				if err != nil {
					return fmt.Errorf("%w: %w", ErrRemovingSSTable, err)
				}
			}
			l.sstables[level] = make([]*sstable.SSTable, 0)

			if level == len(l.sstables)-1 {
				l.sstables = l.sstables[:level+1]
			}

			l.fileCnt++
			if len(l.sstables) == level+1 {
				l.sstables = append(l.sstables, make([]*sstable.SSTable, 0))
			}
			l.sstables[level+1] = append(l.sstables[level+1], newSSTable)
		}
	}

	return nil
}
