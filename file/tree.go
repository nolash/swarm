package file

type treeParams struct {
	SectionSize int
	Branches    int
	Spans       []int
}

func newTreeParams(section int, branches int) *treeParams {

	p := &treeParams{
		SectionSize: section,
		Branches:    branches,
	}
	span := 1
	for i := 0; i < 9; i++ {
		p.Spans = append(p.Spans, span)
		span *= p.Branches
	}
	return p

}
