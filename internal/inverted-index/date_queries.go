package inverted_index

import (
	"errors"
	roaring_bitmap "inverted-index/internal/roaring-bitmap"
	"time"
)

type timeType int

const (
	createdTime timeType = iota
	dieTime
)

var (
	errInvalidRange = errors.New("time end must be after time start")
)

func (i *InvertedIndex) DateQueryCreated(timeStart time.Time, timeEnd time.Time) (roaring_bitmap.Container, error) {
	if timeStart.After(timeEnd) {
		return nil, errInvalidRange
	}
	after, err := i.dateQueryAfter(timeStart, createdTime)
	if err != nil {
		return nil, err
	}
	before, err := i.dateQueryBefore(timeEnd, createdTime)
	if err != nil {
		return nil, err
	}
	return i.And(after, before), nil
}

func (i *InvertedIndex) DateQueryValid(timeStart time.Time, timeEnd time.Time) (roaring_bitmap.Container, error) {
	if timeStart.After(timeEnd) {
		return nil, errInvalidRange
	}
	createdBefore, err := i.dateQueryBefore(timeStart, createdTime)
	if err != nil {
		return nil, err
	}
	dieAfter, err := i.dateQueryAfter(timeEnd, dieTime)
	if err != nil {
		return nil, err
	}
	return i.And(createdBefore, dieAfter), nil
}

func (i *InvertedIndex) dateQueryAfter(timeAfter time.Time, tmType timeType) (roaring_bitmap.Container, error) { // timeAfter is inclusive
	unixTimeAfter := timeAfter.Unix()

	var res roaring_bitmap.Container
	var requiredPrefix roaring_bitmap.Container
	wasFirstSetBit := false

	for j := 63; j >= 0; j-- {
		searchBit := uint16(j)
		if tmType == dieTime {
			searchBit += 64
		}
		currentBitContainer, err := i.storage.Search(searchBit)
		if err != nil {
			return nil, err
		}

		if !wasFirstSetBit && unixTimeAfter&(1<<j) == 0 {
			res = i.Or(res, currentBitContainer)
		} else if unixTimeAfter&(1<<j) == 0 {
			// prefixes are same, timeAfter jth bit set to 0,
			// while current jth bit set to 1 => strictly greater
			res = i.Or(res, i.And(requiredPrefix, currentBitContainer))

			requiredPrefix = i.And(requiredPrefix, i.Not(currentBitContainer))
		} else {
			if !wasFirstSetBit {
				wasFirstSetBit = true
				requiredPrefix = currentBitContainer
			} else {
				requiredPrefix = i.And(requiredPrefix, currentBitContainer)
			}
			if j == 0 {
				res = i.Or(res, requiredPrefix)
			}
		}
	}

	return res, nil
}

func (i *InvertedIndex) dateQueryBefore(timeBefore time.Time, tmType timeType) (roaring_bitmap.Container, error) {
	res, err := i.dateQueryAfter(timeBefore.Add(time.Second), tmType)
	if err != nil {
		return nil, err
	}
	return i.Not(res), nil
}
