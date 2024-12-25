package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type meta struct {
	key         uint16
	cardinality uint16
	offset      uint32
}

func (m *meta) toBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, m.key); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWritingBytes, err)
	}
	if err := binary.Write(buf, binary.LittleEndian, m.cardinality); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWritingBytes, err)
	}
	if err := binary.Write(buf, binary.LittleEndian, m.offset); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWritingBytes, err)
	}

	return buf.Bytes(), nil
}

func metaFromBytes(reader io.Reader) (*meta, error) {
	var key, cardinality uint16
	var offset uint32

	if err := binary.Read(reader, binary.LittleEndian, &key); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &cardinality); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
	}

	return &meta{
		key:         key,
		cardinality: cardinality,
		offset:      offset,
	}, nil
}

func setMetaFileOffset(file *os.File, cookie *cookieData, elementIdx int64) error {
	offset := cookie.getBytesSize() + elementIdx*int64(binary.Size(meta{}))
	_, err := file.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFileSeeking, err)
	}
	return nil
}
