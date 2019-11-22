package file

import (
	"sync/atomic"

	"github.com/ethersphere/swarm/bmt"
	"github.com/ethersphere/swarm/log"
)

type target struct {
	size     int32 // bytes written
	sections int32 // sections written
	level    int32 // target level calculated from bytes written against branching factor and sector size
	resultC  chan []byte
	doneC    chan struct{}
}

func newTarget() *target {
	return &target{
		resultC: make(chan []byte),
		doneC:   make(chan struct{}),
	}
}

func (t *target) Set(size int, sections int, level int) {
	atomic.StoreInt32(&t.size, int32(size))
	atomic.StoreInt32(&t.sections, int32(sections))
	atomic.StoreInt32(&t.level, int32(level))
	close(t.doneC)
}

func (t *target) Count() int {
	return int(atomic.LoadInt32(&t.sections))
}

func (t *target) Done() <-chan []byte {
	return t.resultC
}

type jobUnit struct {
	index int
	data  []byte
}

type job struct {
	target        *target
	level         int // level in tree
	dataSection   int // data section index
	levelSection  int // level section index
	cursorSection int32
	writeC        chan jobUnit
	completeC     chan struct{}
	writer        bmt.SectionWriter // underlying data processor
}

func newJob(params *treeParams, tgt *target, writer bmt.SectionWriter, lvl int, dataSection int) *job {
	jb := &job{
		level:       lvl,
		dataSection: dataSection,
		writer:      writer,
		writeC:      make(chan jobUnit),
		target:      tgt,
	}

	jb.levelSection = dataSectionToLevelSection(params, lvl, dataSection)
	go jb.process()
	return jb
}

func (jb *job) inc() int {
	return int(atomic.AddInt32(&jb.cursorSection, 1))
}

func (jb *job) count() int {
	return int(atomic.LoadInt32(&jb.cursorSection))
}

func (jb *job) write(index int, data []byte) {
	jb.writeC <- jobUnit{
		index: index,
		data:  data,
	}
}

func (jb *job) process() {
OUTER:
	for {
		select {
		case entry := <-jb.writeC:
			newCount := jb.inc()
			targetCount := jb.target.Count()
			log.Trace("write", "newcount", newCount, "targetcount", targetCount)
			jb.writer.Write(entry.index, entry.data)
			if newCount == targetCount {
				break OUTER
			}
		case <-jb.target.doneC:
			count := jb.count()
			targetCount := jb.target.Count()
			log.Trace("complete", "count", count, "targetcount", targetCount)
			if count == targetCount {
				break OUTER
			}
		}
	}
	ref := jb.writer.Sum(nil, 0, nil)
	jb.target.resultC <- ref
}
