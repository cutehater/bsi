package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/bits-and-blooms/bitset"

	"inverted-index/internal/roaring-bitmap"
)

type TableElement struct {
	Key   uint16
	Value roaring_bitmap.Container
}

func (e *TableElement) toBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	if _, err := buf.Write(e.Value.SerializeValues()); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWritingBytes, err)
	}

	return buf.Bytes(), nil
}

func tableElementFromFileRandom(metaFile *os.File, dataFile *os.File, cookie *cookieData, elementIdx int64) (*TableElement, error) {
	elementMeta, err := setDataFileOffset(metaFile, dataFile, cookie, elementIdx, false)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSetFileOffset, err)
	}

	dataReader := bufio.NewReader(dataFile)
	element, err := tableElementFromBytes(dataReader, cookie, elementIdx, elementMeta)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
	}

	return element, nil
}

func tableElementFromFileConsecutive(metaReader *bufio.Reader, dataReader *bufio.Reader, cookie *cookieData, elementIdx int64) (*TableElement, error) {
	elementMeta, err := metaFromBytes(metaReader)
	if err != nil {
		return nil, err
	}

	element, err := tableElementFromBytes(dataReader, cookie, elementIdx, elementMeta)
	if err != nil {
		return nil, err
	}

	return element, nil
}

func tableElementFromBytes(reader io.Reader, cookie *cookieData, elementIdx int64, elementMeta *meta) (*TableElement, error) {
	if cookie.isRunContainer(uint16(elementIdx)) {
		var runCount uint16
		if err := binary.Read(reader, binary.LittleEndian, &runCount); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
		}

		values := make([]roaring_bitmap.RunRecord, runCount)
		for i := 0; i < int(runCount); i++ {
			if err := binary.Read(reader, binary.LittleEndian, &values[i].Start); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
			}
			if err := binary.Read(reader, binary.LittleEndian, &values[i].Length); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
			}
		}

		return &TableElement{
			Key: elementMeta.key,
			Value: &roaring_bitmap.Run{
				Cardinality: elementMeta.cardinality,
				Values:      values,
			},
		}, nil
	} else if elementMeta.cardinality <= roaring_bitmap.MaxArraySize {
		values := make([]uint16, elementMeta.cardinality)
		for i := 0; i < int(elementMeta.cardinality); i++ {
			if err := binary.Read(reader, binary.LittleEndian, &values[i]); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
			}
		}

		return &TableElement{
			Key: elementMeta.key,
			Value: &roaring_bitmap.Array{
				Cardinality: elementMeta.cardinality,
				Values:      values,
			},
		}, nil
	} else {
		uint64s := make([]uint64, roaring_bitmap.BitmapWordsSize)
		for i := 0; i < roaring_bitmap.BitmapWordsSize; i++ {
			if err := binary.Read(reader, binary.LittleEndian, &uint64s[i]); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
			}
		}

		return &TableElement{
			Key: elementMeta.key,
			Value: &roaring_bitmap.Bitmap{
				Cardinality: elementMeta.cardinality,
				Values:      bitset.From(uint64s),
			},
		}, nil
	}
}

func setDataFileOffset(metaFile *os.File, dataFile *os.File, cookie *cookieData, elementIdx int64, setCorrectMetaOffset bool) (*meta, error) {
	err := setMetaFileOffset(metaFile, cookie, elementIdx)
	if err != nil {
		return nil, err
	}

	metaReader := bufio.NewReader(metaFile)
	elementMeta, err := metaFromBytes(metaReader)
	if err != nil {
		return nil, err
	}

	_, err = dataFile.Seek(int64(elementMeta.offset), 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFileSeeking, err)
	}

	if setCorrectMetaOffset {
		err = setMetaFileOffset(metaFile, cookie, elementIdx)
		if err != nil {
			return nil, err
		}
	}

	return elementMeta, nil
}
