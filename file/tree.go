package file

import "github.com/ethersphere/swarm/log"

type treeParams struct {
	section  int
	branches int
	spans    []int
}

func newTreeParams(section int, branches int) *treeParams {

	p := &treeParams{
		section:  section,
		branches: branches,
	}
	span := 1
	for i := 0; i < 9; i++ {
		p.spans = append(p.spans, span)
		log.Trace("spantable", "level", i, "v", span)
		span *= p.branches
	}
	return p

}
