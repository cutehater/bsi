package roaring_bitmap

import (
	"encoding/binary"
	"sort"

	"github.com/bits-and-blooms/bitset"
)

type RunRecord struct {
	Start  uint16
	Length uint16
}

type Run struct {
	Cardinality uint16
	Values      []RunRecord
}

func (r *Run) Add(x uint16) bool {
	idx := sort.Search(len(r.Values), func(i int) bool { return x < r.Values[i].Start })
	if idx > 0 && r.Values[idx-1].Start+r.Values[idx-1].Length >= x {
		return true
	}

	r.Cardinality++

	inPrev := idx > 0 && uint(r.Values[idx-1].Start+r.Values[idx-1].Length)+1 == uint(x)
	inNext := idx < len(r.Values) && r.Values[idx].Start-1 == x

	if inPrev && inNext {
		r.Values = append(r.Values[:idx-1], append([]RunRecord{
			{
				Start:  r.Values[idx-1].Start,
				Length: r.Values[idx-1].Length + r.Values[idx].Length + 2,
			},
		}, r.Values[idx+1:]...)...)
	} else if inPrev {
		r.Values[idx-1].Length++
	} else if inNext {
		r.Values[idx].Start--
		r.Values[idx].Length++
	} else {
		r.Values = append(r.Values[:idx], append([]RunRecord{
			{
				Start:  x,
				Length: 0,
			},
		}, r.Values[idx:]...)...)
	}

	return false
}

func (r *Run) GetCardinality() uint16 {
	return r.Cardinality
}

func (r *Run) ConvertToArray() *Array {
	a := Array{
		Cardinality: r.Cardinality,
	}

	for _, value := range r.Values {
		for i := 0; i <= int(value.Length); i++ {
			a.Values = append(a.Values, value.Start+uint16(i))
		}
	}

	return &a
}

func (r *Run) ConvertToBitmap() *Bitmap {
	b := Bitmap{
		Cardinality: r.Cardinality,
		Values:      bitset.New(bitmapSize),
	}

	for _, value := range r.Values {
		b.Values.FlipRange(uint(value.Start), uint(value.Start+value.Length)+1)
	}

	return &b
}

func (r *Run) ConvertToRun() *Run {
	return r
}

func (r *Run) CountNumberOfRuns() uint16 {
	return uint16(len(r.Values))
}

func (r *Run) SerializeValues() []byte {
	byteArray := make([]byte, len(r.Values)*4)
	for i, record := range r.Values {
		binary.LittleEndian.PutUint16(byteArray[i*4:], record.Start)
		binary.LittleEndian.PutUint16(byteArray[i*4+2:], record.Length)
	}
	return byteArray
}
