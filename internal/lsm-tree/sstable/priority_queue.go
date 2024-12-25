package sstable

type mergeItem struct {
	value      TableElement
	readerIdx  int
	elementIdx int64
}

type priorityQueue []*mergeItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	if pq[i].value.Key < pq[j].value.Key {
		return true
	}
	return pq[i].value.Key == pq[j].value.Key && pq[i].readerIdx > pq[j].readerIdx
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	element := x.(*mergeItem)
	*pq = append(*pq, element)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	element := old[n-1]
	*pq = old[0 : n-1]
	return element
}
