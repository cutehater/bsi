package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bits-and-blooms/bitset"

	roaring_bitmap "inverted-index/internal/roaring-bitmap"
)

type cookieData struct {
	runFlagBitset *bitset.BitSet
}

func (c *cookieData) isRunContainer(elementIdx uint16) bool {
	return c.runFlagBitset.Test(uint(elementIdx))
}

func (c *cookieData) getBytesSize() int64 {
	return roaring_bitmap.BitmapWordsSize * 8
}

func (c *cookieData) toBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	b := make([]byte, roaring_bitmap.BitmapWordsSize*8)
	for i, val := range c.runFlagBitset.Bytes() {
		binary.LittleEndian.PutUint64(b[i*8:], val)
	}

	if _, err := buf.Write(b); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWritingBytes, err)
	}

	return buf.Bytes(), nil
}

func cookieFromBytes(reader io.Reader) (*cookieData, error) {
	uint64s := make([]uint64, 0, roaring_bitmap.BitmapWordsSize)
	buf := make([]byte, 8)
	readBytes := 0

	for len(uint64s) < roaring_bitmap.BitmapWordsSize {
		n, err := io.ReadFull(reader, buf[readBytes:])
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
		}
		readBytes += n

		if readBytes == 8 {
			uint64s = append(uint64s, binary.LittleEndian.Uint64(buf))
			readBytes = 0
		}
	}

	return &cookieData{
		runFlagBitset: bitset.From(uint64s),
	}, nil
}
