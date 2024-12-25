package roaring_bitmap

import (
	"sort"

	"github.com/bits-and-blooms/bitset"
)

func And(c1 Container, c2 Container) Container {
	wasSwap := false
	defer func() {
		if wasSwap {
			c1, c2 = c2, c1
		}
	}()

	if c1 == nil || c2 == nil {
		return nil
	}

	if _, ok := c2.(*Array); ok {
		wasSwap = !wasSwap
		c1, c2 = c2, c1
	}
	if _, ok := c2.(*Bitmap); ok {
		if _, ok = c1.(*Run); ok {
			wasSwap = !wasSwap
			c1, c2 = c2, c1
		}
	}

	var result Container

	switch c1.(type) {
	case *Array:
		a := c1.(*Array).Values

		switch c2.(type) {
		case *Array:
			aOther := c2.(*Array).Values

			if len(a) > len(aOther) {
				wasSwap = !wasSwap
				a, aOther = aOther, a
			}

			values := make([]uint16, 0, min(len(a), len(aOther)))

			if len(a)*gallopingMergingThreshold > len(aOther) {
				var i, j int
				for i < len(a) && j < len(aOther) {
					if a[i] < aOther[j] {
						i++
					} else if a[i] > aOther[j] {
						j++
					} else {
						values = append(values, a[i])
						i++
						j++
					}
				}
			} else {
				for _, v := range a {
					idx := sort.Search(len(aOther), func(i int) bool { return aOther[i] >= v })
					if idx == len(aOther) {
						break
					} else if aOther[idx] == v {
						values = append(values, v)
					}
				}
			}

			if len(values) == 0 {
				return nil
			}
			result = &Array{
				Cardinality: uint16(len(values) - 1),
				Values:      values,
			}
		case *Bitmap:
			b := c2.(*Bitmap).Values

			cardinality := uint16(0)
			values := bitset.New(bitmapSize)

			for _, v := range a {
				if b.Test(uint(v)) {
					values.Set(uint(v))
					cardinality++
				}
			}

			if cardinality == 0 {
				return nil
			}
			result = &Bitmap{
				Cardinality: cardinality - 1,
				Values:      values,
			}
		case *Run:
			r := c2.(*Run).Values

			values := make([]uint16, 0, len(a))

			var i, j int
			for i < len(a) && j < len(r) {
				if r[j].Start+r[j].Length < a[i] {
					j++
				} else {
					if a[i] >= r[j].Start {
						values = append(values, a[i])
					}
					i++
				}
			}

			if len(values) == 0 {
				return nil
			}
			result = &Array{
				Cardinality: uint16(len(values) - 1),
				Values:      values,
			}
		}
	case *Bitmap:
		b := c1.(*Bitmap).Values

		switch c2.(type) {
		case *Bitmap:
			values := b.Intersection(c2.(*Bitmap).Values)

			if !values.Any() {
				return nil
			}
			result = &Bitmap{
				Cardinality: uint16(values.Count() - 1),
				Values:      values,
			}
		case *Run:
			r := c2.(*Run).Values

			if uint(c2.GetCardinality())+1 <= MaxArraySize {
				values := make([]uint16, 0, c2.GetCardinality()+1)

				for _, v := range r {
					for i := uint16(0); i <= v.Length; i++ {
						if b.Test(uint(v.Start + i)) {
							values = append(values, v.Start+i)
						}
					}
				}

				if len(values) == 0 {
					return nil
				}
				result = &Array{
					Cardinality: uint16(len(values) - 1),
					Values:      values,
				}
			} else {
				values := bitset.New(bitmapSize)

				for _, v := range r {
					values.FlipRange(uint(v.Start), uint(v.Start+v.Length))
				}
				values.InPlaceIntersection(b)

				if !values.Any() {
					return nil
				}
				result = &Bitmap{
					Cardinality: uint16(values.Count() - 1),
					Values:      values,
				}
			}
		}
	case *Run:
		if c1.GetCardinality() == bitmapSize-1 && c2.GetCardinality() == bitmapSize-1 {
			result = c1
			break
		}

		r := c1.(*Run).Values
		rOther := c2.(*Run).Values

		cardinality := uint16(0)
		values := make([]RunRecord, 0, len(r)+len(rOther))

		var i, j int
		for i < len(r) && j < len(rOther) {
			if r[i].Start <= rOther[j].Start {
				if r[i].Start+r[i].Length < rOther[j].Start {
					i++
				} else if r[i].Start+r[i].Length <= rOther[j].Start+rOther[j].Length {
					cardinality += (r[i].Start + r[i].Length) - rOther[j].Start + 1
					values = append(values, RunRecord{
						Start:  rOther[j].Start,
						Length: (r[i].Start + r[i].Length) - rOther[j].Start,
					})
					if r[i].Start+r[i].Length == rOther[j].Start+rOther[j].Length {
						j++
					}
					i++
				} else {
					cardinality += rOther[j].Length + 1
					values = append(values, rOther[j])
					j++
				}
			} else {
				if rOther[j].Start+rOther[j].Length < r[i].Start {
					j++
				} else if rOther[j].Start+rOther[j].Length <= r[i].Start+r[i].Length {
					cardinality += (rOther[j].Start + rOther[j].Length) - r[i].Start + 1
					values = append(values, RunRecord{
						Start:  r[i].Start,
						Length: (rOther[j].Start + rOther[j].Length) - r[i].Start,
					})
					if rOther[j].Start+rOther[j].Length == r[i].Start+r[i].Length {
						i++
					}
					j++
				} else {
					cardinality += r[i].Length + 1
					values = append(values, r[i])
					i++
				}
			}
		}

		if len(values) == 0 {
			return nil
		}
		result = &Run{
			Cardinality: cardinality - 1,
			Values:      values,
		}
	}

	return convertToBestType(result)
}

func Or(c1 Container, c2 Container) Container {
	wasSwap := false
	defer func() {
		if wasSwap {
			c1, c2 = c2, c1
		}
	}()

	if c1 == nil {
		return c2
	} else if c2 == nil {
		return c1
	}

	if _, ok := c2.(*Array); ok {
		wasSwap = !wasSwap
		c1, c2 = c2, c1
	}
	if _, ok := c2.(*Bitmap); ok {
		if _, ok = c1.(*Run); ok {
			wasSwap = !wasSwap
			c1, c2 = c2, c1
		}
	}

	var result Container

	switch c1.(type) {
	case *Array:
		a := c1.(*Array).Values

		switch c2.(type) {
		case *Array:
			aOther := c2.(*Array).Values

			if c1.GetCardinality()+c2.GetCardinality()+2 <= MaxArraySize {
				values := make([]uint16, 0, len(a)+len(aOther))

				j := 0
				for i := 0; i < len(a); i++ {
					for j < len(aOther) && aOther[j] < a[i] {
						if len(values) == 0 || values[len(values)-1] != aOther[j] {
							values = append(values, aOther[j])
						}
						j++
					}

					if len(values) == 0 || values[len(values)-1] != a[i] {
						values = append(values, a[i])
					}
				}
				for j < len(aOther) {
					if len(values) == 0 || values[len(values)-1] != aOther[j] {
						values = append(values, aOther[j])
					}
					j++
				}

				result = &Array{
					Cardinality: uint16(len(values) - 1),
					Values:      values,
				}
			} else {
				cardinality := c1.GetCardinality()
				values := bitset.New(bitmapSize)

				for _, v := range a {
					values.Set(uint(v))
				}
				for _, v := range aOther {
					if !values.Test(uint(v)) {
						values.Set(uint(v))
						cardinality++
					}
				}

				result = &Bitmap{
					Cardinality: cardinality,
					Values:      values,
				}
			}
		case *Bitmap:
			b := c2.(*Bitmap).Values

			cardinality := c2.GetCardinality()
			values := bitset.New(bitmapSize)
			values.Copy(b)

			for _, v := range a {
				if !values.Test(uint(v)) {
					values.Set(uint(v))
					cardinality++
				}
			}

			result = &Bitmap{
				Cardinality: cardinality,
				Values:      values,
			}
		case *Run:
			r := c2.(*Run).Values

			cardinality := 0
			values := make([]RunRecord, 0, len(a)+len(r))

			lastRecord := RunRecord{}
			var i, j int
			for i < len(a) || j < len(r) {
				if i < len(a) && (j == len(r) || a[i] <= r[j].Start) {
					if i == 0 && j == 0 {
						lastRecord.Start = a[i]
						lastRecord.Length = 0
					} else if uint(lastRecord.Start+lastRecord.Length)+1 < uint(a[i]) {
						cardinality += int(lastRecord.Length) + 1
						values = append(values, lastRecord)
						lastRecord.Start = a[i]
						lastRecord.Length = 0
					} else if uint(lastRecord.Start+lastRecord.Length)+1 == uint(a[i]) {
						lastRecord.Length++
					}
					i++
				} else {
					if i == 0 && j == 0 {
						lastRecord = r[j]
					} else if uint(lastRecord.Start+lastRecord.Length)+1 < uint(r[j].Start) {
						cardinality += int(lastRecord.Length) + 1
						values = append(values, lastRecord)
						lastRecord = r[j]
					} else if lastRecord.Start+lastRecord.Length < r[j].Start+r[j].Length {
						lastRecord.Length += (r[j].Start + r[j].Length) - (lastRecord.Start + lastRecord.Length)
					}
					j++
				}
			}
			if lastRecord.Length > 0 {
				cardinality += int(lastRecord.Length) + 1
				values = append(values, lastRecord)
			}

			result = &Run{
				Cardinality: uint16(cardinality - 1),
				Values:      values,
			}
		}
	case *Bitmap:
		switch c2.(type) {
		case *Bitmap:
			values := c1.(*Bitmap).Values.Union(c2.(*Bitmap).Values)

			result = &Bitmap{
				Cardinality: uint16(values.Count() - 1),
				Values:      values,
			}
		case *Run:
			values := bitset.New(bitmapSize)

			for _, v := range c2.(*Run).Values {
				values.FlipRange(uint(v.Start), uint(v.Start+v.Length)+1)
			}
			values.InPlaceUnion(c1.(*Bitmap).Values)

			result = &Bitmap{
				Cardinality: uint16(values.Count() - 1),
				Values:      values,
			}
		}
	case *Run:
		r := c1.(*Run).Values
		rOther := c2.(*Run).Values

		cardinality := 0
		values := make([]RunRecord, 0, len(r)+len(rOther))

		lastRecord := RunRecord{}
		var i, j int
		for i < len(r) && j < len(rOther) {
			if i < len(r) && (j == len(rOther) || r[i].Start <= rOther[j].Start) {
				if i == 0 && j == 0 {
					lastRecord = r[i]
				} else if uint(lastRecord.Start+lastRecord.Length)+1 < uint(r[i].Start) {
					cardinality += int(lastRecord.Length) + 1
					values = append(values, lastRecord)
					lastRecord = r[i]
				} else if lastRecord.Start+lastRecord.Length < r[i].Start+r[i].Length {
					lastRecord.Length += (r[i].Start + r[i].Length) - (lastRecord.Start + lastRecord.Length)
				}
				i++
			} else {
				if i == 0 && j == 0 {
					lastRecord = rOther[j]
				} else if uint(lastRecord.Start+lastRecord.Length)+1 < uint(rOther[j].Start) {
					cardinality += int(lastRecord.Length) + 1
					values = append(values, lastRecord)
					lastRecord = rOther[j]
				} else if lastRecord.Start+lastRecord.Length < rOther[j].Start+rOther[j].Length {
					lastRecord.Length += (rOther[j].Start + rOther[j].Length) - (lastRecord.Start + lastRecord.Length)
				}
				j++
			}
		}
		if lastRecord.Length > 0 {
			cardinality += int(lastRecord.Length) + 1
			values = append(values, lastRecord)
		}

		result = &Run{
			Cardinality: uint16(cardinality - 1),
			Values:      values,
		}
	}

	return convertToBestType(result)
}

func Not(c Container, docsCount uint16) Container {
	var result Container

	switch c.(type) {
	case nil:
		values := bitset.New(bitmapSize)
		values.SetAll()
		values.FlipRange(uint(docsCount), bitmapSize)

		result = &Bitmap{
			Cardinality: docsCount - 1,
			Values:      values,
		}
	case *Array:
		values := bitset.New(bitmapSize)
		values.SetAll()
		values.FlipRange(uint(docsCount), bitmapSize)

		for _, v := range c.(*Array).Values {
			values.Clear(uint(v))
		}

		result = &Bitmap{
			Cardinality: docsCount - 2 - c.GetCardinality(),
			Values:      values,
		}
	case *Bitmap:
		temp := bitset.New(bitmapSize)
		temp.SetAll()
		temp.FlipRange(uint(docsCount), bitmapSize)

		values := c.(*Bitmap).Values.Complement()
		values.InPlaceIntersection(temp)
		result = &Bitmap{
			Cardinality: docsCount - 2 - c.GetCardinality(),
			Values:      values,
		}
	case *Run:
		r := c.(*Run).Values

		values := make([]RunRecord, 0, len(r)+1)

		for i, v := range r {
			if i == 0 {
				if v.Start > 0 {
					values = append(values, RunRecord{
						Start:  0,
						Length: v.Start - 1,
					})
				}
			} else {
				values = append(values, RunRecord{
					Start:  r[i-1].Start + r[i-1].Length + 1,
					Length: v.Start - (r[i-1].Start + r[i-1].Length) - 2,
				})
			}
		}
		if r[len(r)-1].Start+r[len(r)-1].Length < bitmapSize-1 {
			values = append(values, RunRecord{
				Start:  r[len(r)-1].Start + r[len(r)-1].Length + 1,
				Length: bitmapSize - 1 - (r[len(r)-1].Start + 1 + r[len(r)-1].Length + 1),
			})
		}

		result = &Run{
			Cardinality: docsCount - 2 - c.GetCardinality(),
			Values:      values,
		}
	}

	return convertToBestType(result)
}
