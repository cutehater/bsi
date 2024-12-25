package roaring_bitmap

const (
	bitmapSize                         = 1 << 16
	BitmapWordsSize                    = 1 << 10
	MaxArraySize                       = 4096
	maxNumberOfRunsToConvertFromBitmap = 2047
	gallopingMergingThreshold          = 64
)
