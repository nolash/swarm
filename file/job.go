package file

import (
	"github.com/ethersphere/swarm/bmt"
	"github.com/ethersphere/swarm/log"
)

type hasherParams struct {
	section  int
	branches int
	spans    []int
}

type target struct {
	size     int // bytes written
	sections int // sections written
	level    int // target level calculated from bytes written against branching factor and sector size
}

func newHasherParams(section int, branches int) *hasherParams {

	p := &hasherParams{
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

type job struct {
	level      int // level in tree
	startData  int // data section index
	startLevel int // level section index
}

func newJob(params *hasherParams, tgt *target, writer bmt.SectionWriter, level int, dataSection int) *job {
	return &job{
		level: level,
	}
}
