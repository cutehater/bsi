package roaring_bitmap

import (
	"encoding/binary"
	"sort"

	"github.com/bits-and-blooms/bitset"
)

type Array struct {
	Cardinality uint16
	Values      []uint16
}

func (a *Array) Add(x uint16) bool {
	idx := sort.Search(len(a.Values), func(i int) bool { return x <= a.Values[i] })
	if idx < len(a.Values) && x == a.Values[idx] {
		return true
	}

	a.Values = append(a.Values[:idx], append([]uint16{x}, a.Values[idx:]...)...)
	a.Cardinality++

	return false
}

func (a *Array) GetCardinality() uint16 {
	return a.Cardinality
}

func (a *Array) ConvertToArray() *Array {
	return a
}

func (a *Array) ConvertToBitmap() *Bitmap {
	b := Bitmap{
		Cardinality: a.Cardinality,
		Values:      bitset.New(bitmapSize),
	}

	for _, value := range a.Values {
		b.Values.Set(uint(value))
	}

	return &b
}

func (a *Array) ConvertToRun() *Run {
	r := Run{
		Cardinality: a.Cardinality,
	}

	for _, value := range a.Values {
		if len(r.Values) == 0 {
			r.Values = append(r.Values, RunRecord{Start: value, Length: 0})
			continue
		}

		last := r.Values[len(r.Values)-1]
		if last.Start+last.Length == value {
			r.Values[len(r.Values)-1].Length++
		} else {
			r.Values = append(r.Values, RunRecord{Start: value, Length: 0})
		}
	}

	return &r
}

func (a *Array) CountNumberOfRuns() uint16 {
	ans := 0

	for i, value := range a.Values {
		if i == 0 || value != a.Values[i-1]+1 {
			ans++
		}
	}

	return uint16(ans)
}

func (a *Array) SerializeValues() []byte {
	byteArray := make([]byte, len(a.Values)*2)
	for i, v := range a.Values {
		binary.LittleEndian.PutUint16(byteArray[i*2:], v)
	}
	return byteArray
}
