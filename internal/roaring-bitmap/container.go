package roaring_bitmap

type Container interface {
	Add(uint16) bool
	ConvertToArray() *Array
	ConvertToBitmap() *Bitmap
	ConvertToRun() *Run
	CountNumberOfRuns() uint16
	GetCardinality() uint16
	SerializeValues() []byte
}

func convertToBestType(c Container) Container {
	if c == nil {
		return nil
	}

	if uint(c.GetCardinality())+1 <= MaxArraySize {
		if uint(c.CountNumberOfRuns()) < (uint(c.GetCardinality())+2)/2 {
			return c.ConvertToRun()
		} else {
			return c.ConvertToArray()
		}
	} else {
		if c.CountNumberOfRuns() < maxNumberOfRunsToConvertFromBitmap {
			return c.ConvertToRun()
		} else {
			return c.ConvertToBitmap()
		}
	}
}
