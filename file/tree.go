package file

import "github.com/ethersphere/swarm/bmt"

type treeParams struct {
	SectionSize int
	Branches    int
	Spans       []int
	hashFunc    func() bmt.SectionWriter
}

func newTreeParams(section int, branches int, hashFunc func() bmt.SectionWriter) *treeParams {

	p := &treeParams{
		SectionSize: section,
		Branches:    branches,
		hashFunc:    hashFunc,
	}
	span := 1
	for i := 0; i < 9; i++ {
		p.Spans = append(p.Spans, span)
		span *= p.Branches
	}
	return p

}
