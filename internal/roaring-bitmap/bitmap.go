package roaring_bitmap

import (
	"encoding/binary"

	"github.com/bits-and-blooms/bitset"
)

type Bitmap struct {
	Cardinality uint16
	Values      *bitset.BitSet
}

func (b *Bitmap) Add(x uint16) bool {
	if b.Values.Test(uint(x)) {
		return true
	}

	b.Values.Set(uint(x))
	b.Cardinality++

	return false
}

func (b *Bitmap) GetCardinality() uint16 {
	return b.Cardinality
}

func (b *Bitmap) ConvertToArray() *Array {
	a := Array{
		Cardinality: b.Cardinality,
	}

	for i, e := b.Values.NextSet(0); e; i, e = b.Values.NextSet(i + 1) {
		a.Values = append(a.Values, uint16(i))
	}

	return &a
}

func (b *Bitmap) ConvertToBitmap() *Bitmap {
	return b
}

func (b *Bitmap) ConvertToRun() *Run {
	r := Run{
		Cardinality: b.Cardinality,
	}

	var i uint
	var e bool
	for i, e = b.Values.NextSet(0); e; i, e = b.Values.NextSet(i + 1) {
		record := RunRecord{
			Start: uint16(i),
		}

		i, e = b.Values.NextClear(i)
		if e == false {
			i = bitmapSize
		}

		i--
		record.Length = uint16(i) - record.Start
		r.Values = append(r.Values, record)
	}

	return &r
}

func (b *Bitmap) CountNumberOfRuns() uint16 {
	bcopy := bitset.New(bitmapSize)
	_ = b.Values.Copy(bcopy)

	bcopy.ShiftLeft(1)
	bcopy.FlipRange(0, bitmapSize)
	bcopy.InPlaceIntersection(b.Values)

	return uint16(b.Values.Count())
}

func (b *Bitmap) SerializeValues() []byte {
	uint64Array := b.Values.Bytes()

	byteArray := make([]byte, bitmapSize*8)
	for i, v := range uint64Array {
		binary.LittleEndian.PutUint64(byteArray[i*8:], v)
	}

	return byteArray
}
