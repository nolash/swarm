package storage

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethersphere/swarm/bmt"
	"github.com/ethersphere/swarm/log"
)

// defines the chained writer interface
type SectionHasherTwo interface {
	bmt.SectionWriter
	BatchSize() uint64 // sections to write before sum should be called
	PadSize() uint64   // additional sections that will be written on sum
}

// index to retrieve already existing parent jobs
type level struct {
	height int32
	jobs   map[uint64]*hashJobTwo
}

// encapsulates a single chunk write
type hashJobTwo struct {
	parent           *hashJobTwo
	level            int32            // tree level of job
	levelIndex       uint64           // chunk index in own level
	firstDataSection uint64           // first data section index this job points to
	dataSize         uint64           // data size at the time of Sum() call, used to calculate the correct span value
	parentSection    uint64           // section index in parent job this job will write its hash to
	count            uint64           // number of writes currently made to this job
	targetLevel      int32            // target level, set when Sum() is called
	targetCount      uint64           // target section on this level, set when Sum() is called
	writer           SectionHasherTwo // underlying hasher
}

// Implements Stringer
func (h *hashJobTwo) String() string {
	return fmt.Sprintf("%p", h)
}

func (h *hashJobTwo) log(s string) {
	dataSize := atomic.LoadUint64(&h.dataSize)
	targetLevel := atomic.LoadInt32(&h.targetLevel)
	count := atomic.LoadUint64(&h.count)
	log.Trace(s, "level", h.level, "levelindex", h.levelIndex, "firstdatasection", h.firstDataSection, "parentsection", h.parentSection, "datasize", dataSize, "count", count, "targetLevel", targetLevel, "targetCount", h.targetCount, "p", fmt.Sprintf("%p", h))
}

// writes to underlying writer
// does not protect against double writes or writes out of index range
func (m *FileSplitterTwo) write(job *hashJobTwo, index int, b []byte) {
	job.log(fmt.Sprintf("job write: %d,%x", index, b))
	if uint64(index) > m.branches-1 {
		panic(fmt.Sprintf("got index %d", index))
	}
	targetLevel := atomic.LoadInt32(&job.targetLevel)
	count := atomic.AddUint64(&job.count, 1)
	if job.firstDataSection == 0 && count == 1 {
		m.topHashC <- topHash{
			level:     job.level,
			reference: b,
		}
	}
	if targetLevel == job.level {
		m.freeJob(job)
		return
	}
	job.writer.Write(index, b)

	// TODO: targetCount match must be hardened, it is possible for it not to have been set before the last job in a level reaches here
	targetCount := atomic.LoadUint64(&job.targetCount)
	if count == m.branches || count == targetCount {
		log.Debug("doing job sum", "level", job.level, "dataSection", job.firstDataSection, "count", count, "targetcount", targetCount)
		go m.sum(job)
	}
}

// executes sum on the written data and writes result to corresponding parent section
// creates parent object if it is missing
func (m *FileSplitterTwo) sum(job *hashJobTwo) {

	sectionSize := uint64(m.SectionSize())

	// dataSize is only set if the job is the last write job or a parent of it
	dataSize := atomic.LoadUint64(&job.dataSize)

	// if dataSize is 0 it follows that all chunks below are full
	// we can then calculate the size of the data under the span
	// from the amount of sections written and which level it is on
	count := atomic.LoadUint64(&job.count)
	lvl := uint64(job.level)
	if dataSize == 0 {
		dataSize = count * m.spanTable[lvl] * m.sectionSize
	} else {
		spanBytes := m.spanTable[lvl] * m.sectionSize
		dataSize = dataSize - spanBytes + (count-1)*spanBytes
	}

	// span is the serialized size data embedded in the chunk
	span := lengthToSpan(dataSize)

	// perform the hashing
	// thisRefsSize is the length of the actual hash input data to be hashed
	thisRefsSize := count * sectionSize
	result := job.writer.Sum(nil, int(thisRefsSize), span)
	job.log(fmt.Sprintf("job sum: %v, %x", span, result))

	// write to parent corresponding index
	parent := m.getOrCreateParent(job)
	go m.write(parent, int(job.parentSection%m.branches), result)
	m.freeJob(job)
}

type topHash struct {
	level     int32
	reference []byte
}

// FileSplitter manages the build tree of the data
type FileSplitterTwo struct {
	branches        uint64            // cached branch count
	sectionSize     uint64            // cached segment size of writer
	chunkSize       uint64            // cached chunk size
	writerBatchSize uint64            // cached chunk size of chained writer
	parentBatchSize uint64            // cached number of writes before change parent
	writerPadSize   uint64            // cached padding size of the chained writer
	balancedTable   map[uint64]uint64 // maps write counts to bytecounts for
	spanTable       []uint64          // cached table of span section counts per level

	levels      map[int32]level
	lastJob     *hashJobTwo // keeps pointer to the job for the first reference level
	lastWrite   uint64      // total number of bytes currently written
	lastCount   uint64      // total number of sections currently written
	targetLevel int32       // top level of hash tree

	topHashC chan topHash
	resultC  chan []byte

	dataHasher *bmt.Hasher
	getWriter  func() SectionHasherTwo // mode-dependent function to assign hasher
	putWriter  func(SectionHasherTwo)  // mode-dependent function to release hasher
	writerFunc func() SectionHasherTwo // hasher function used by manual and GC modes
	writerMu   sync.Mutex

	writerPool sync.Pool // chained writers providing hashing in Pool mode
}

func NewFileSplitterTwo(dataHasher *bmt.Hasher, writerFunc func() SectionHasherTwo) (*FileSplitterTwo, error) {

	if writerFunc == nil {
		return nil, errors.New("writer cannot be nil")
	}

	// create new instance and cache frequenctly used values
	writer := writerFunc()
	branches := writer.BatchSize() + writer.PadSize()
	f := &FileSplitterTwo{
		levels:          make(map[int32]level),
		branches:        branches,
		sectionSize:     uint64(writer.SectionSize()),
		chunkSize:       branches * uint64(writer.SectionSize()),
		writerBatchSize: writer.BatchSize(),
		parentBatchSize: writer.BatchSize() * branches,
		writerPadSize:   writer.PadSize(),
		balancedTable:   make(map[uint64]uint64),
		writerFunc:      writerFunc,
		dataHasher:      dataHasher,
		topHashC:        make(chan topHash, 9),
		resultC:         make(chan []byte),
	}

	f.writerPool.New = func() interface{} {
		return writerFunc()
	}
	f.getWriter = f.getWriterPool
	f.putWriter = f.putWriterPool

	f.Reset()

	// create lookup table for data write counts that result in balanced trees
	lastBoundary := uint64(1)
	f.balancedTable[lastBoundary] = uint64(f.sectionSize)
	for i := 1; i < 9; i++ {
		lastBoundary *= uint64(f.branches)
		f.balancedTable[lastBoundary] = lastBoundary * uint64(f.sectionSize)
		log.Trace("balancedtable", "boundary", lastBoundary, "v", f.balancedTable[lastBoundary])
	}

	span := uint64(1)
	for i := 0; i < 9; i++ {
		f.spanTable = append(f.spanTable, span)
		log.Trace("spantable", "level", i, "v", span)
		span *= f.branches
	}

	return f, nil
}

// free allocated resources to a job
func (m *FileSplitterTwo) freeJob(job *hashJobTwo) {
	m.writerMu.Lock()
	defer m.writerMu.Unlock()
	delete(m.levels[job.level].jobs, job.levelIndex)
	m.putWriter(job.writer)
}

// will be called on starts cascading
// shortLength is the size of the last chunk on data level
// if shortLength is zero, the full chunk length will be used
func (m *FileSplitterTwo) finish() {

	// get the last level that will be written to
	m.targetLevel = int32(getLevelsFromLength(m.lastWrite, m.sectionSize, m.branches) - 1)
	m.writerMu.Lock()
	job := m.lastJob
	atomic.StoreUint64(&job.dataSize, m.lastWrite%(m.branches*m.chunkSize))
	atomic.StoreInt32(&job.targetLevel, m.targetLevel)

	// calculate target count
	targetCount := m.lastCount - job.firstDataSection
	job.targetCount = targetCount
	log.Debug("finish", "short", job.dataSize, "targetLevel", job.targetLevel, "targetcount", targetCount, "lastCount", m.lastCount, "firstdataSection", job.firstDataSection)
	m.writerMu.Unlock()

	level := int32(1) // first level of parent
	for level != m.targetLevel {
		parent := m.getOrCreateParent(job)
		//parentTargetCount := m.getJobCountFromDataCount(targetCount, parent.level)
		parentTargetCount := m.lastCount/m.spanTable[parent.level] + 1
		atomic.StoreInt32(&parent.targetLevel, m.targetLevel)
		atomic.StoreUint64(&parent.targetCount, parentTargetCount)
		log.Trace("finish parent", "targetLevel", m.targetLevel, "targetcount", parentTargetCount, "level", parent.level)
		job = parent
		level += 1
	}

}

// creates a new hash job object
// when generating a parent job the parentSection of the child is passed and used to calculate the consecutive parent index
func (m *FileSplitterTwo) newHashJobTwo(level int32, dataSectionIndex uint64, thisSectionIndex uint64) *hashJobTwo {
	job := &hashJobTwo{
		level:            level,
		firstDataSection: dataSectionIndex,
		writer:           m.getWriter(),
	}

	// parentSection is the section to write to in the parent
	// calculated from the section the current job starts at
	var parentSection uint64
	if thisSectionIndex > 0 {
		parentSection = m.getParentSection(thisSectionIndex)
		atomic.StoreUint64(&job.parentSection, parentSection)
	}
	levelIndex := m.getIndexFromSection(parentSection)
	m.writerMu.Lock()
	m.levels[level].jobs[levelIndex] = job
	job.log(fmt.Sprintf("add job: levelindex %d", levelIndex))
	m.writerMu.Unlock()
	return job
}

// creates a new hash parent job object
func (m *FileSplitterTwo) getOrCreateParent(job *hashJobTwo) *hashJobTwo {

	// first, check if parent already exists and return if it does return it
	// TODO: consider whether it is useful to have parent member on job since it will only be used once
	m.writerMu.Lock()
	parent := job.parent
	m.writerMu.Unlock()
	if parent == nil {
		var ok bool
		// second, check the levels
		// if parent already was created by a different job under the same span
		// if it was return it
		parentSection := atomic.LoadUint64(&job.parentSection)
		levelIndex := m.getIndexFromSection(parentSection)
		m.writerMu.Lock()
		level := job.level
		parent, ok = m.levels[level+1].jobs[levelIndex]
		m.writerMu.Unlock()
		if ok {
			log.Trace("level index has parent", "levelindex", levelIndex, "level", level)
			m.writerMu.Lock()
			job.parent = parent
			m.writerMu.Unlock()
		} else {

			// if no parent exists create a new job for it
			// first calculate the data index that corresponds to the start section of this job
			parentFirstDataBoundary := m.getLowerBoundaryByLevel(job.firstDataSection, job.level+1)
			job.log(fmt.Sprintf("creating new parent: %d", parentFirstDataBoundary))
			// the job constructor adds the job to the level index
			parent = m.newHashJobTwo(job.level+1, parentFirstDataBoundary, parentSection)

			// assign the parent to the child job parent member for quick access later
			m.writerMu.Lock()
			job.parent = parent
			m.writerMu.Unlock()
		}
		if job.dataSize > 0 {
			parent.dataSize = job.dataSize
		}
	} else {
		log.Trace("job has parent")
	}
	return parent
}

// implements SectionHasherTwo
// BUT
// currently it is wired up to read 4096 bytes per call
// logic needs to be added to call the Sum() only if enough sections have been written
func (m *FileSplitterTwo) Write(index int, b []byte) {

	// update write index and byte count for data lavel
	sectionWrites := ((len(b) - 1) / 32) + 1
	m.lastCount += uint64(sectionWrites)
	m.lastWrite += uint64(len(b))
	if index == 0 {
		log.Info("FIRST WRITE")
	}

	// synchronously hash data
	span := lengthToSpan(uint64(len(b)))
	m.dataHasher.ResetWithLength(span)
	m.dataHasher.Write(b)
	ref := m.dataHasher.Sum(nil)
	log.Trace("Write()", "index", index, "lastwrite", m.lastWrite, "lastcount", m.lastCount, "sectionsinwrite", sectionWrites, "w", fmt.Sprintf("%p", m.lastJob.writer), "h", hexutil.Encode(ref), "span", span)

	// write the hash to the first level of intermediate chunks
	job := m.lastJob
	// if on a chunk boundary on level 1, trigger a sum on that level
	// create and attach a new job for level 1
	// sum() will free the existing job
	if m.lastCount%(m.branches*m.branches) == 0 {
		log.Trace("batch threshold")
		m.lastJob = m.newHashJobTwo(1, m.lastCount, m.lastCount/m.branches)
	}
	// TODO: it should be possible to put this in a goroutine as long as the original job is preserved and passed to sum/write
	// but when putting in goroutine
	m.write(job, (index/int(m.branches))%int(m.branches), ref)
}

// implements SectionHasherTwo
//
// BUG: putting m.finish() only to be executed when tree is not balanced hangs the result
// but there is no coordination that the targetLevel will be set before the cascading hashers in case of balanced tree will complete
// we may need a channel trigger for spewing out the tophash
//
// also consider alternative implementation with buffered channels
func (m *FileSplitterTwo) Sum(b []byte, length int, span []byte) []byte {

	log.Debug("Sum()", "writes", m.lastWrite, "count", m.lastCount)

	// if less than chunkSize bytes have been written merely return the already stored hash
	if m.lastWrite <= m.chunkSize {
		m.targetLevel = 1
	} else {
		// mark the current job as the final one and sum
		m.finish()
	}
	if v, ok := m.balancedTable[m.lastCount]; !ok {
		go m.sum(m.lastJob)
	} else {
		log.Debug("tree is balanced", "v", v)
	}

	// add quitC for context cancel (and timeout?)
	for {
		select {
		case h := <-m.topHashC:
			log.Trace("got tophash", "level", h.level, "ref", hexutil.Encode(h.reference))
			if h.level == m.targetLevel {
				return h.reference
			}
		}
	}
	// wait for result from write on target level
	return nil
}

// implements SectionHasherTwo
func (m *FileSplitterTwo) Reset() {
	m.lastCount = 0
	m.lastWrite = 0
	close(m.topHashC)
	m.topHashC = make(chan topHash, 9)
	if m.lastJob != nil {
		m.freeJob(m.lastJob)
	}
	for i := int32(0); i < 9; i++ {
		for _, job := range m.levels[i].jobs {
			m.freeJob(job)
		}
		m.levels[i] = level{
			height: i,
			jobs:   make(map[uint64]*hashJobTwo),
		}
	}
	m.lastJob = m.newHashJobTwo(1, 0, 0)
}

// see writerMode consts
func (m *FileSplitterTwo) getWriterPool() SectionHasherTwo {
	return m.writerPool.Get().(SectionHasherTwo)
}

func (m *FileSplitterTwo) putWriterPool(writer SectionHasherTwo) {
	writer.Reset()
	m.writerPool.Put(writer)
}

// implements SectionHasherTwo
func (m *FileSplitterTwo) BatchSize() uint64 {
	return m.writerBatchSize + m.writerPadSize
}

// implements SectionHasherTwo
func (m *FileSplitterTwo) PadSize() uint64 {
	return 0
}

// implements SectionHasherTwo
func (m *FileSplitterTwo) SectionSize() int {
	return int(m.sectionSize)
}
