package file

import (
	"sync/atomic"

	"github.com/ethersphere/swarm/bmt"
)

type target struct {
	size     int // bytes written
	sections int // sections written
	level    int // target level calculated from bytes written against branching factor and sector size
}

type job struct {
	level        int // level in tree
	dataSection  int // data section index
	levelSection int // level section index
	count        int32
	writer       bmt.SectionWriter // underlying data processor
}

func newJob(params *treeParams, tgt *target, writer bmt.SectionWriter, lvl int, dataSection int) *job {
	j := &job{
		level:       lvl,
		dataSection: dataSection,
		writer:      writer,
	}

	j.levelSection = dataSectionToLevelSection(params, lvl, dataSection)
	return j
}

func (jb *job) write(index int, data []byte) {
	atomic.AddInt32(&jb.count, 1)
	jb.writer.Write(index, data)
}
