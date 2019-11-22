package file

import (
	"github.com/ethersphere/swarm/bmt"
)

type target struct {
	size     int // bytes written
	sections int // sections written
	level    int // target level calculated from bytes written against branching factor and sector size
}

type job struct {
	level      int // level in tree
	startData  int // data section index
	startLevel int // level section index
}

func newJob(params *treeParams, tgt *target, writer bmt.SectionWriter, level int, dataSection int) *job {
	return &job{
		level: level,
	}
}
