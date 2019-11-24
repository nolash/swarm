package file

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethersphere/swarm/bmt"
	"github.com/ethersphere/swarm/log"
)

type jobIndex struct {
	maxLevels int
	jobs      []sync.Map
	mu        sync.Mutex
}

func newJobIndex(maxLevels int) *jobIndex {
	ji := &jobIndex{
		maxLevels: maxLevels,
	}
	for i := 0; i < maxLevels; i++ {
		ji.jobs = append(ji.jobs, sync.Map{})
	}
	return ji
}

func (ji *jobIndex) Add(jb *job) {
	log.Trace("adding job", "job", jb)
	ji.jobs[jb.level].Store(jb.dataSection, jb)
}

func (ji *jobIndex) Get(lvl int, section int) *job {
	jb, ok := ji.jobs[lvl].Load(section)
	if !ok {
		return nil
	}
	return jb.(*job)
}

func (ji *jobIndex) Delete(jb *job) {
	ji.jobs[jb.level].Delete(jb.dataSection)
}

type target struct {
	size     int32 // bytes written
	sections int32 // sections written
	level    int32 // target level calculated from bytes written against branching factor and sector size
	resultC  chan []byte
	doneC    chan struct{}
	abortC   chan struct{}
}

func newTarget() *target {
	return &target{
		resultC: make(chan []byte),
		doneC:   make(chan struct{}),
		abortC:  make(chan struct{}),
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

func (t *target) Abort() {
	t.size = 0
	t.sections = 0
	t.level = 0
	close(t.abortC)
}

func (t *target) Cleanup() {
	close(t.abortC)
}

type jobUnit struct {
	index int
	data  []byte
}

// encapsulates one single chunk to be hashed
type job struct {
	target *target
	params *treeParams
	index  *jobIndex

	level           int   // level in tree
	dataSection     int   // data section index
	levelSection    int   // level section index
	cursorSection   int32 // next write position in job
	endCount        int32 // number of writes to be written to this job (0 means write to capacity)
	lastSectionSize int   // data size on the last data section write

	writeC chan jobUnit
	writer bmt.SectionWriter // underlying data processor
}

func newJob(params *treeParams, tgt *target, jobIndex *jobIndex, lvl int, dataSection int) *job {
	jb := &job{
		params:      params,
		index:       jobIndex,
		level:       lvl,
		dataSection: dataSection,
		writer:      params.hashFunc(),
		writeC:      make(chan jobUnit),
		target:      tgt,
	}
	if jb.index == nil {
		jb.index = newJobIndex(9)
	}

	jb.levelSection = dataSectionToLevelSection(params, lvl, dataSection)
	jb.index.Add(jb)
	go jb.process()
	return jb
}

func (jb *job) String() string {
	return fmt.Sprintf("job: l:%d,s:%d,c:%d", jb.level, jb.dataSection, jb.count())
}

func (jb *job) inc() int {
	return int(atomic.AddInt32(&jb.cursorSection, 1))
}

func (jb *job) count() int {
	return int(atomic.LoadInt32(&jb.cursorSection))
}

// size returns the byte size of the span the job represents
// if job is last index in a level and writes have been finalized, it will return the target size
// otherwise, regardless of job index, it will return the size according to the current write count
// TODO: returning expected size in one case and actual size in another can lead to confusion
func (jb *job) size() int {
	count := jb.count()
	endCount := int(atomic.LoadInt32(&jb.endCount))
	if endCount == 0 {
		return count * jb.params.SectionSize * jb.params.Spans[jb.level]
	}
	log.Trace("size", "sections", jb.target.sections)
	return int(jb.target.size) % (jb.params.Spans[jb.level] * jb.params.SectionSize * jb.params.Branches)
}

// add data to job
// does no checking for data length or index validity
func (jb *job) write(index int, data []byte) {
	jb.writeC <- jobUnit{
		index: index,
		data:  data,
	}
}

// determine whether the given data section count falls within the span of the current job
func (jb *job) targetWithinJob(targetSection int) (int, bool) {
	var endCount int
	var ok bool

	// span one level above equals the data size of 128 units of one section on this level
	// using the span table saves one multiplication
	upperLimit := jb.dataSection + jb.params.Spans[jb.level+1]

	// the data section is the data section index where the span of this job starts
	if targetSection >= jb.dataSection && targetSection < upperLimit {

		// data section index must be divided by corresponding section size on the job's level
		// then wrap on branch period to find the correct section within this job
		endCount = (targetSection / jb.params.Spans[jb.level]) % jb.params.Branches

		ok = true
	}
	log.Trace("within", "upper", upperLimit, "target", targetSection, "endcount", endCount, "ok", ok)
	return int(endCount), ok
}

// runs in loop until:
// - sectionSize number of job writes have occurred (one full chunk)
// - data write is finalized and targetcount for this chunk was already reached
// - data write is finalized and targetcount is reached on a subsequent job write
func (jb *job) process() {

	// is set when data write is finished, AND
	// the final data section falls within the span of this job
	// if not, loop will only exit on Branches writes
	endCount := 0
OUTER:
	for {
		select {

		// if aborted we exit immediately
		case <-jb.target.abortC:
			log.Debug("hasher job aborted")
			jb.target.resultC <- nil
			return

		// enter here if new data is written to the job
		case entry := <-jb.writeC:
			newCount := jb.inc()
			// this write is superfluous when the received data is the root hash
			jb.writer.Write(entry.index, entry.data)
			log.Trace("job write", "level", jb.level, "count", newCount, "index", entry.index, "data", hexutil.Encode(entry.data))

			// this is the full chunk write condition, and most common (when larger files are being written)
			if newCount == jb.params.Branches {
				break OUTER
			}

			// since newcount is incremented above it can only equal endcount if this has been set in the case below,
			// which means data write has been completed
			if newCount == endCount {

				// expect one write on target level means we have the root hash
				if endCount == 1 && int(jb.target.level) == jb.level {
					jb.target.resultC <- entry.data
					jb.target.Cleanup()
					return
				}
				break OUTER
			}

		// enter here if data writes have been completed
		// TODO: this case currently executes for all cycles after data write is complete for which writes to this job do not happen. perhaps it can be improved
		case <-jb.target.doneC:

			// we can never have count 0 and have a completed job
			// this is the easiest check we can make
			log.Trace("doneloop", "level", jb.level, "count", jb.count())
			count := jb.count()
			if count == 0 {
				continue
			}

			// if we have reached the end count for this chunk, we proceed to hashing
			// this case is important when write to the level happen after this goroutine
			// registers that data writes have been completed
			if count == int(endCount) {
				log.Warn("breaking donec", "count", count, "endcount", endCount, "level", jb.level)
				break OUTER
			}

			// if endcount is already calculated, don't calculate it again
			if endCount > 0 {
				continue
			}

			// if the target count falls within the span of this job
			// set the endcount so we know we have to do extra calculations for
			// determining span in case of unbalanced tree
			// TODO: consider whether we can ALWAYS make the endcount to a value > 0 on the last chunk, which means we avoid the endcount=0 condition check on balanced tree further down
			targetCount := jb.target.Count()
			endCount = jb.targetCountToEndCount(targetCount)
			atomic.StoreInt32(&jb.endCount, int32(endCount))
		}
	}

	// get the size of the span and execute the hash digest of the content
	size := jb.size()
	span := lengthToSpan(size)
	log.Trace("job sum", "count", jb.count(), "size", size, "span", span, "level", jb.level, "targetlevel", jb.target.level, "endcount", endCount)
	ref := jb.writer.Sum(nil, size, span)

	// endCount > 0 means this is the last chunk on the level
	// the hash from the level below the target level will be the result
	if endCount > 0 && int(jb.target.level-1) == jb.level {
		jb.target.resultC <- ref
		jb.target.Cleanup()
		return
	}

	// retrieve the parent and the corresponding section in it to write to
	parent := jb.parent()
	parentSection := dataSectionToLevelSection(jb.params, 1, jb.dataSection)
	parent.write(parentSection, ref)

	// job is done. We don't look back and boldly free up its resources
	jb.index.Delete(jb)
}

// if last data index falls within the span, return the appropriate end count for the level
// otherwise return 0 (which means job write until limit)
func (jb *job) targetCountToEndCount(targetCount int) int {
	endIndex, ok := jb.targetWithinJob(targetCount - 1)
	if !ok {
		return 0
	}
	return endIndex + 1
}

// returns the parent job of the receiver job
// a new parent job is created if none exists for the slot
func (jb *job) parent() *job {
	newLevel := jb.level + 1
	spanDivisor := jb.params.Spans[jb.level]
	// Truncate to even quotient which is the actual logarithmic boundary of the data section under the span
	newDataSection := ((dataSectionToLevelSection(jb.params, 1, jb.dataSection)) / spanDivisor) * spanDivisor
	parent := jb.index.Get(newLevel, newDataSection)
	if parent != nil {
		return parent
	}
	return newJob(jb.params, jb.target, jb.index, jb.level+1, newDataSection)
}

// Next creates the job for the next data section span on the same level as the receiver job
// this is only meant to be called once for each job, consequtive calls will overwrite index with new empty job
func (jb *job) Next() *job {
	return newJob(jb.params, jb.target, jb.index, jb.level, jb.dataSection+jb.params.Spans[jb.level])
}
