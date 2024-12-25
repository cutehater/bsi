package inverted_index

import roaring_bitmap "inverted-index/internal/roaring-bitmap"

func (i *InvertedIndex) And(c1 roaring_bitmap.Container, c2 roaring_bitmap.Container) roaring_bitmap.Container {
	return roaring_bitmap.And(c1, c2)
}

func (i *InvertedIndex) Or(c1 roaring_bitmap.Container, c2 roaring_bitmap.Container) roaring_bitmap.Container {
	return roaring_bitmap.Or(c1, c2)
}

func (i *InvertedIndex) Not(c roaring_bitmap.Container) roaring_bitmap.Container {
	return roaring_bitmap.Not(c, i.documentsNumber)
}
