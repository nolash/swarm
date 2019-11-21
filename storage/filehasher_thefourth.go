package storage

import (
	"encoding/binary"
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

// creates a binary span size representation
// to pass to bmt.SectionWriter
// TODO: move to bmt.SectionWriter, which is the object actually using this
func lengthToSpan(l uint64) []byte {
	spanBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(spanBytes, l)
	return spanBytes
}

// index to retrieve already existing parent jobs
type level struct {
	height int
	jobs   map[uint64]*hashJobTwo
}

// encapsulates a single chunk write
type hashJobTwo struct {
	parent           *hashJobTwo
	level            int              // tree level of job
	levelIndex       uint64           // chunk index in own level
	firstDataSection uint64           // first data section index this job points to
	dataSize         uint64           // size of data underlying span represents
	parentSection    uint64           // section index in parent job this job will write its hash to
	count            uint64           // number of writes currently made to this job
	finish           bool             // set on last level 1 job when Sum() is called
	writer           SectionHasherTwo // underlying hasher
}

// Implements Stringer
func (h *hashJobTwo) String() string {
	return fmt.Sprintf("%p", h)
}

// writes to underlying writer
// does not protect against double writes or writes out of index range
func (m *FileSplitterTwo) write(job *hashJobTwo, index int, b []byte) {
	job.writer.Write(index, b)
	count := atomic.AddUint64(&job.count, 1)
	// TODO: this will write on every hash write to every chunk on the 0 dataindex side, can be optimized
	if job.firstDataSection == 0 && count == 1 {
		m.setTopHash(b)
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
	if dataSize == 0 {
		level := uint64(job.level)
		count := atomic.LoadUint64(&job.count)
		dataSize = count * (level * m.branches * m.sectionSize)
	}

	// calculate the necessary SectionWriter parameters
	span := lengthToSpan(dataSize)
	count := atomic.LoadUint64(&job.count)
	thisRefsSize := count * sectionSize

	// execute sum
	r := job.writer.Sum(nil, int(thisRefsSize), span)

	// write to parent corresponding index
	parent := m.getOrCreateParent(job)
	m.write(parent, int(job.parentSection), r)
	m.freeJob(job)
}

// FileSplitter manages the build tree of the data
type FileSplitterTwo struct {
	branches        uint64 // cached branch count
	sectionSize     uint64 // cached segment size of writer
	chunkSize       uint64
	writerBatchSize uint64            // cached chunk size of chained writer
	parentBatchSize uint64            // cached number of writes before change parent
	writerPadSize   uint64            // cached padding size of the chained writer
	balancedTable   map[uint64]uint64 // maps write counts to bytecounts for

	levels      map[int]level
	lastJob     *hashJobTwo // keeps pointer to the job for the first reference level
	topHash     []byte
	topJob      *hashJobTwo
	lastWrite   uint64 // total number of bytes currently written
	lastCount   uint64 // total number of sections currently written
	targetCount uint64 // set when sum is called, is total number of bytes finally written
	targetLevel int32  // set when sum is called, is tree level of root chunk

	resultC chan []byte

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
		levels:          make(map[int]level),
		branches:        branches,
		sectionSize:     uint64(writer.SectionSize()),
		chunkSize:       branches * uint64(writer.SectionSize()),
		writerBatchSize: writer.BatchSize(),
		parentBatchSize: writer.BatchSize() * branches,
		writerPadSize:   writer.PadSize(),
		balancedTable:   make(map[uint64]uint64),
		writerFunc:      writerFunc,
		dataHasher:      dataHasher,
		topHash:         make([]byte, int(writer.SectionSize())),
		resultC:         make(chan []byte),
	}

	for i := 0; i < 9; i++ {
		f.levels[i] = level{
			height: i,
			jobs:   make(map[uint64]*hashJobTwo),
		}
	}
	f.writerPool.New = func() interface{} {
		return writerFunc()
	}
	f.getWriter = f.getWriterPool
	f.putWriter = f.putWriterPool

	// create lookup table for data write counts that result in balanced trees
	lastBoundary := uint64(1)
	f.balancedTable[lastBoundary] = uint64(f.sectionSize)
	for i := 1; i < 9; i++ {
		lastBoundary *= uint64(f.branches)
		f.balancedTable[lastBoundary] = lastBoundary * uint64(f.sectionSize)
	}

	// create the hasherJob object for the data level.
	f.lastJob = f.newHashJobTwo(1, 0, 0)

	return f, nil
}

// calculates amount of sections the given data affects
func sectionCount(b []byte, sectionSize uint64) uint64 {
	return uint64(len(b)-1)/sectionSize + 1
}

// returns number of sections a slice of data comprises
// rounded up to nearest sectionsize boundary
func (m *FileSplitterTwo) sectionCount(b []byte) uint64 {
	return sectionCount(b, uint64(m.SectionSize()))
}

// calculates section of the parent job in its level that will be written to
func (m *FileSplitterTwo) getParentSection(idx uint64) uint64 {
	return idx / m.branches
}

// calculates the lower branch boundary to the corresponding section
func (m *FileSplitterTwo) getIndexFromSection(sectionCount uint64) uint64 {
	return sectionCount / m.branches
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
func (m *FileSplitterTwo) finish(shortLength uint64) {
	m.writerMu.Lock()
	defer m.writerMu.Unlock()
	job := m.lastJob
	job.finish = true
	log.Debug("finish", "short", shortLength)
}

// creates a new hash job object
// when generating a parent job the parentSection of the child is passed and used to calculate the consecutive parent index
func (m *FileSplitterTwo) newHashJobTwo(level int, dataSectionIndex uint64, thisSectionIndex uint64) *hashJobTwo {
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
	log.Trace("add job", "job", job, "levelindex", levelIndex)
	m.writerMu.Unlock()
	return job
}

// creates a new hash parent job object
func (m *FileSplitterTwo) getOrCreateParent(job *hashJobTwo) *hashJobTwo {

	// first, check if parent already exists
	// TODO: consider whether it is useful to have parent member on job since it will only be used once
	m.writerMu.Lock()
	parent := job.parent
	m.writerMu.Unlock()
	if parent != nil {
		log.Trace("job has parent")
		return parent
	}

	// second, check the levels index
	// if parent already was created by a different job under the same span
	parentSection := atomic.LoadUint64(&job.parentSection)
	levelIndex := m.getIndexFromSection(parentSection)
	m.writerMu.Lock()
	parent, ok := m.levels[job.level+1].jobs[levelIndex]
	m.writerMu.Unlock()
	if ok {
		log.Trace("level index has parent", "levelindex", levelIndex)
		m.writerMu.Lock()
		job.parent = parent
		m.writerMu.Unlock()
		return parent
	}

	// lastly create a new job for the parent
	// the job constructor adds the job to the level index
	log.Trace("creating new parent")
	parent = m.newHashJobTwo(job.level+1, job.firstDataSection, job.parentSection)
	m.writerMu.Lock()
	job.parent = parent
	m.writerMu.Unlock()
	return parent
}

// see writerMode consts
func (m *FileSplitterTwo) getWriterPool() SectionHasherTwo {
	return m.writerPool.Get().(SectionHasherTwo)
}

// see writerMode consts
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

func (m *FileSplitterTwo) setTopHash(b []byte) {
	m.writerMu.Lock()
	defer m.writerMu.Unlock()
	log.Trace("setting tophash", "hash", hexutil.Encode(b))
	copy(m.topHash, b)
}

// implements SectionHasherTwo
func (m *FileSplitterTwo) Write(index int, b []byte) {

	sectionWrites := ((len(b) - 1) / 32) + 1
	m.lastCount += uint64(sectionWrites)
	m.lastWrite += uint64(len(b))
	log.Trace("Write()", "offset", index, "lastwrite", m.lastWrite, "lastcount", m.lastCount, "sectionsinwrite", sectionWrites, "w", fmt.Sprintf("%p", m.lastJob.writer))
	span := lengthToSpan(uint64(len(b)))
	m.dataHasher.ResetWithLength(span)
	m.dataHasher.Write(b)
	ref := m.dataHasher.Sum(nil)
	log.Trace("summed data", "h", hexutil.Encode(ref), "span", span)
	if index == 0 {
		m.setTopHash(ref)
	}
	m.write(m.lastJob, index%int(m.branches), ref)
	if m.lastCount%m.chunkSize == 0 {
		go m.sum(m.lastJob)
		m.lastJob = m.newHashJobTwo(1, m.lastCount, m.lastCount)
	}
}

// implements SectionHasherTwo
func (m *FileSplitterTwo) Sum(b []byte, length int, span []byte) []byte {

	log.Debug("Sum()", "writes", m.lastWrite)
	if m.lastWrite <= m.chunkSize {
		return m.topHash
	}

	shortLength := m.lastWrite % m.chunkSize
	m.finish(shortLength)
	m.sum(m.lastJob)

	//if m.lastWrite <= uint64(m.sectionSize*m.branches) {
	//	return m.topHash
	//	}
	//count := atomic.LoadInt32(&m.lastJob.count)
	//m.sum(b, int(count-1), count-1, m.lastJob, m.lastJob.writer, m.lastJob.parent)
	return <-m.resultC
}

// implements SectionHasherTwo
func (m *FileSplitterTwo) Reset() {
	log.Warn("filesplitter Reset() is unimplemented")
}
