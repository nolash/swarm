package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/crypto/sha3"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethersphere/swarm/bmt"
	"github.com/ethersphere/swarm/log"
)

const (
	defaultPadSize     = 18
	defaultSegmentSize = 32
)

const (
	writerModePool   = iota // use sync.Pool for managing hasher allocation
	writerModeGC            // only allocate new hashers, rely on GC to reap them
	writerModeManual        // handle a pre-allocated hasher pool with buffered channels
)

var (
	hashPool            sync.Pool
	mockPadding         = [defaultPadSize * defaultSegmentSize]byte{}
	FileHasherAlgorithm = DefaultHash
)

func init() {
	for i := 0; i < len(mockPadding); i++ {
		mockPadding[i] = 0x01
	}
	hashPool.New = func() interface{} {

		pool := bmt.NewTreePool(sha3.NewLegacyKeccak256, 128, bmt.PoolSize)
		h := bmt.New(pool)
		return h.NewAsyncWriter(false)
	}
}

func getHasher() bmt.SectionWriter {
	return hashPool.Get().(bmt.SectionWriter)
}

func putHasher(h bmt.SectionWriter) {
	h.Reset()
	hashPool.Put(h)
}

// defines the chained writer interface
type SectionHasherTwo interface {
	bmt.SectionWriter
	BatchSize() uint64 // sections to write before sum should be called
	PadSize() uint64   // additional sections that will be written on sum
}

// used for benchmarks against pyramid hasher which uses sync hasher
type treeHasherWrapper struct {
	*bmt.Hasher
	zeroLength []byte
	mu         sync.Mutex
}

func newTreeHasherWrapper() *treeHasherWrapper {
	pool := bmt.NewTreePool(sha3.NewLegacyKeccak256, 128, bmt.PoolSize)
	h := bmt.New(pool)
	return &treeHasherWrapper{
		Hasher:     h,
		zeroLength: make([]byte, 8),
	}
}

// implements SectionHasherTwo
func (h *treeHasherWrapper) Write(index int, b []byte) {
	h.Hasher.Write(b)
}

// implements SectionHasherTwo
func (h *treeHasherWrapper) Sum(b []byte, length int, span []byte) []byte {
	return h.Hasher.Sum(b)
}

// implements SectionHasherTwo
func (h *treeHasherWrapper) BatchSize() uint64 {
	return 128
}

// implements SectionHasherTwo
func (h *treeHasherWrapper) PadSize() uint64 {
	return 0
}

// implements SectionHasherTwo
func (h *treeHasherWrapper) SectionSize() int {
	return 32
}

func (h *treeHasherWrapper) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Hasher.ResetWithLength(h.zeroLength)
}

// FileChunker is a chainable FileHasher writer that creates chunks on write and sum
// TODO not implemented
type FileChunker struct {
	branches uint64
}

func NewFileChunker() *FileChunker {
	return &FileChunker{
		branches: 128,
	}

}

// implements SectionHasherTwo
func (f *FileChunker) Write(index int, b []byte) {
	log.Trace("got write", "b", len(b))
}

// implements SectionHasherTwo
func (f *FileChunker) Sum(b []byte, length int, span []byte) []byte {
	log.Warn("got sum", "b", hexutil.Encode(b), "span", span)
	return b[:f.SectionSize()]
}

// implements SectionHasherTwo
func (f *FileChunker) BatchSize() uint64 {
	return f.branches
}

// implements SectionHasherTwo
func (f *FileChunker) PadSize() uint64 {
	return 0
}

// implements SectionHasherTwo
func (f *FileChunker) SectionSize() int {
	return 32
}

// implements SectionHasherTwo
func (f *FileChunker) Reset() {
	return
}

// FilePadder is a chainable FileHasher writer that pads the data written to it on sum
// illustrates possible erasure coding interface
type FilePadder struct {
	hasher bmt.SectionWriter
	writer SectionHasherTwo
	buffer []byte
	limit  int // write count limit (in segments)
	writeC chan int
}

func NewFilePadder(writer SectionHasherTwo) *FilePadder {
	if writer == nil {
		panic("writer can't be nil")
	}
	p := &FilePadder{
		writer: writer,
		limit:  110,
	}

	p.writeC = make(chan int, writer.BatchSize())
	p.Reset()
	return p
}

// implements SectionHasherTwo
func (p *FilePadder) BatchSize() uint64 {
	return p.writer.BatchSize() - p.PadSize()
}

// implements SectionHasherTwo
func (p *FilePadder) PadSize() uint64 {
	return 18
}

// implements SectionHasherTwo
func (p *FilePadder) Size() int {
	return p.hasher.SectionSize()
}

// implements SectionHasherTwo
// ignores index
// TODO bmt.SectionWriter.Write interface should return write count
func (p *FilePadder) Write(index int, b []byte) {
	//log.Debug("padder write", "index", index, "l", len(b), "c", atomic.AddUint64(&p.debugSize, uint64(len(b))))
	log.Debug("padder write", "index", index, "l", len(b))
	if index > p.limit {
		panic(fmt.Sprintf("write index beyond limit; %d > %d", index, p.limit))
	}
	p.hasher.Write(index, b)
	p.writeBuffer(index, b)
	p.writeC <- len(b)
}

func (p *FilePadder) writeBuffer(index int, b []byte) {
	bytesIndex := index * p.SectionSize()
	copy(p.buffer[bytesIndex:], b[:p.SectionSize()])
}

// implements SectionHasherTwo
// ignores span
func (p *FilePadder) Sum(b []byte, length int, span []byte) []byte {
	var writeCount int
	select {
	case c, ok := <-p.writeC:
		if !ok {
			break
		}
		writeCount += c
		if writeCount == length {
			break
		}
	}

	// at this point we are not concurrent anymore
	// TODO optimize
	padding := p.pad(nil)
	for i := 0; i < len(padding); i += p.hasher.SectionSize() {
		log.Debug("padder pad", "i", i, "limit", p.limit)
		p.hasher.Write(p.limit, padding[i:])
		p.writeBuffer(p.limit, padding[i:])
		p.limit++
	}
	s := p.hasher.Sum(b, length+len(padding), span)
	//p.writer.Sum(append(s, p.buffer...), length, span)
	chunk := NewChunk(Address(s), p.buffer)
	log.Warn("have chunk", "chunk", chunk, "chunkdata", chunk.Data())
	putHasher(p.hasher)
	return s
}

// implements SectionHasherTwo
func (p *FilePadder) Reset() {
	p.hasher = getHasher()
	p.buffer = make([]byte, (p.PadSize()+p.BatchSize())*uint64(p.SectionSize()))
}

// implements SectionHasherTwo
// panics if called after sum and before reset
func (p *FilePadder) SectionSize() int {
	return p.hasher.SectionSize()
}

// performs data padding on the supplied data
// returns padding
func (p *FilePadder) pad(b []byte) []byte {
	return mockPadding[:]
}

// utility structure for controlling asynchronous tree hashing of the file
type hasherJob struct {
	parent        *hasherJob
	dataOffset    uint64 // global write count this job represents
	levelOffset   uint64 // offset on this level
	count         uint64 // amount of writes on this job
	edge          int    // > 0 on last write, incremented by 1 every level traversed on right edge, used to determine skipping levels on dangling chunk
	level         int
	debugHash     []byte
	debugLifetime uint32
	writer        SectionHasherTwo
}

func (h *hasherJob) inc() (uint64, uint64) {
	oldCount := atomic.LoadUint64(&h.count)
	newCount := atomic.AddUint64(&h.count, 1)
	return oldCount, newCount
}

// FileSplitter manages the build tree of the data
type FileSplitter struct {
	branches        int               // cached branch count
	sectionSize     int               // cached segment size of writer
	writerBatchSize uint64            // cached chunk size of chained writer
	parentBatchSize uint64            // cached number of writes before change parent
	writerPadSize   uint64            // cached padding size of the chained writer
	balancedTable   map[uint64]uint64 // maps write counts to bytecounts for

	topHash     []byte  // caches the last hash written to the first data index on the top level
	lastJob     *altJob // keeps pointer to the job for the first reference level
	lastWrite   uint64  // total number of bytes currently written
	lastCount   uint64  // total number of sections currently written
	targetCount uint64  // set when sum is called, is total number of bytes finally written
	targetLevel int     // set when sum is called, is tree level of root chunk

	resultC chan []byte

	dataHasher *bmt.Hasher
	getWriter  func() SectionHasherTwo // mode-dependent function to assign hasher
	putWriter  func(SectionHasherTwo)  // mode-dependent function to release hasher
	writerFunc func() SectionHasherTwo // hasher function used by manual and GC modes
	writerMu   sync.Mutex

	writerPool sync.Pool // chained writers providing hashing in Pool mode
}

func NewFileSplitter(dataHasher *bmt.Hasher, writerFunc func() SectionHasherTwo, mode int) (*FileSplitter, error) {

	if writerFunc == nil {
		return nil, errors.New("writer cannot be nil")
	}

	// create new instance and cache frequenctly used values
	writer := writerFunc()
	branches := writer.BatchSize() + writer.PadSize()
	f := &FileSplitter{
		branches:        int(branches),
		sectionSize:     writer.SectionSize(),
		writerBatchSize: writer.BatchSize(),
		parentBatchSize: writer.BatchSize() * branches,
		writerPadSize:   writer.PadSize(),
		balancedTable:   make(map[uint64]uint64),
		writerFunc:      writerFunc,
		dataHasher:      dataHasher,
		resultC:         make(chan []byte),
		topHash:         make([]byte, writer.SectionSize()),
	}

	// see writerMode*
	switch mode {
	case writerModePool:
		f.writerPool.New = func() interface{} {
			return writerFunc()
		}
		f.getWriter = f.getWriterPool
		f.putWriter = f.putWriterPool
	default:
		return nil, fmt.Errorf("invalid mode")
	}

	// create lookup table for data write counts that result in balanced trees
	lastBoundary := uint64(1)
	f.balancedTable[lastBoundary] = uint64(f.sectionSize)
	for i := 1; i < 9; i++ {
		lastBoundary *= uint64(f.branches)
		f.balancedTable[lastBoundary] = lastBoundary * uint64(f.sectionSize)
	}

	// create the hasherJob object for the data level.
	f.lastJob = &altJob{
		level:  1,
		writer: f.getWriter(),
	}

	return f, nil
}

func (m *FileSplitter) Result() []byte {
	return <-m.resultC
}

// implements SectionHasherTwo
func (m *FileSplitter) BatchSize() uint64 {
	return m.writerBatchSize + m.writerPadSize
}

// implements SectionHasherTwo
func (m *FileSplitter) PadSize() uint64 {
	return 0
}

// implements SectionHasherTwo
func (m *FileSplitter) SectionSize() int {
	return m.sectionSize
}

// implements SectionHasherTwo
func (m *FileSplitter) Write(index int, b []byte) {

	sectionWrites := ((len(b) - 1) / 32) + 1
	m.lastCount += uint64(sectionWrites)
	m.lastWrite += uint64(len(b))
	log.Trace("data write", "offset", index, "lastwrite", m.lastWrite, "lastcount", m.lastCount, "sectionsinwrite", sectionWrites, "w", fmt.Sprintf("%p", m.lastJob.writer))
	span := lengthToSpan(uint64(len(b)))
	m.dataHasher.ResetWithLength(span)
	m.dataHasher.Write(b)
	ref := m.dataHasher.Sum(nil)
	if m.lastWrite <= uint64(m.sectionSize*m.branches) {
		copy(m.topHash, ref)
	}
	log.Trace("summed data", "h", fmt.Sprintf("%x", ref), "span", span)
	m.write(m.lastJob, index%m.branches, ref)
}

// implements SectionHasherTwo
// TODO is noop
func (m *FileSplitter) Sum(b []byte, length int, span []byte) []byte {

	m.targetCount = m.lastCount
	for i := m.lastCount; i > 0; i /= 128 {
		m.targetLevel += 1
	}
	log.Debug("set targetlevel", "l", m.targetLevel)
	m.lastJob.edge = 1
	if m.lastWrite <= uint64(m.sectionSize*m.branches) {
		return m.topHash
	}
	m.sum(b, int(m.lastJob.count-1), m.lastJob.count-1, m.lastJob, m.lastJob.writer, m.lastJob.parent)
	return <-m.resultC
}

// implements SectionHasherTwo
// TODO is noop
func (m *FileSplitter) Reset() {
	log.Warn("filesplitter reset called, not implemented")
}

func lengthToSpan(l uint64) []byte {
	spanBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(spanBytes, l)
	return spanBytes
}

// handles recursive writing across tree levels
// b byte is not thread safe
// index is internal within a job (batchsize / sectionsize)
func (m *FileSplitter) write(h *altJob, index int, b []byte) {

	// if we are crossing a batch write size, we spawn a new job
	// and point the data writer's job pointer lastJob to it
	oldcount, newcount := h.inc()

	// write the data to the chain
	m.writerMu.Lock()
	w := h.writer
	m.writerMu.Unlock()
	w.Write(index, b)
	log.Debug("job write", "level", h.level, "index", index, "bytes", hexutil.Encode(b), "job", fmt.Sprintf("%p", h), "w", fmt.Sprintf("%p", w), "edge", h.edge)

	// sum data if:
	// * the write is on a threshold, or
	// * if we're done writing
	if newcount == m.writerBatchSize || h.edge > 0 {
		// we use oldcount here to do one less operation when calculating thisJobLength
		go m.sum(b, index, oldcount, h, w, h.parent)

		// TODO edge need to be set here when we implement the right edge finish write
		m.reset(h, m.getWriter())
	}
}

// handles recursive feedback writes of chained sum call
// as the hasherJob from the context calling this is asynchronously reset
// the relevant values to use for calculation must be copied
// if parent doesn't exist (new level) a new one is created
// releases the hasher used by the hasherJob at time of calling this method
//func (m *FileSplitter) sum(b []byte, index int, oldcount uint64, dataOffset uint64, levelOffset uint64, job *hasherJob, w SectionHasherTwo, p *hasherJob) {
func (m *FileSplitter) sum(b []byte, index int, count uint64, job *altJob, w SectionHasherTwo, p *altJob) {

	// the size of the actual data passed to the hasher
	dataToHashSize := int(count+1) * m.sectionSize

	// the size of the data the resulting hash represents
	dataToSpanSize := count * uint64(m.sectionSize) * uint64(job.level*m.branches)
	if len(b) == 0 {
		// if passed data length is zero means the call came from Sum()
		// we add the last non full chunk of data
		// TODO: If ends on a chunk boundary, nothing will be added and the result will be one chunk short
		tailSize := m.lastWrite % uint64(m.sectionSize*m.branches)
		if tailSize == 0 {
			dataToSpanSize += uint64(m.sectionSize * m.branches)
		} else {
			dataToSpanSize += tailSize
		}
	} else if job.edge > 0 {
		if job.edge == m.targetLevel {
			copy(m.topHash, b)
			m.resultC <- b
			return
		}
		dataToSpanSize += m.lastWrite % uint64(m.sectionSize*m.branches)

	} else {
		// if we have data length it means the call comes from write() and represents a 128 section write
		// however, the last section may be shorter than the section size
		dataToSpanSize += uint64(len(b)) * uint64(job.level*m.branches)
	}

	// span is the total size under the chunk
	spanBytes := lengthToSpan(dataToSpanSize)

	log.Debug("job sum", "bytelength", len(b), "level", job.level, "index", index, "count", count, "jobcount", job.count, "length", dataToHashSize, "span", spanBytes, "job", fmt.Sprintf("%p", job), "w", fmt.Sprintf("%p", w), "edge", job.edge)
	s := w.Sum(
		nil,
		dataToHashSize,
		spanBytes,
	)
	log.Debug("hash result", "s", hexutil.Encode(s), "length", dataToHashSize, "span", spanBytes, "p", fmt.Sprintf("%p", p))
	if index == 0 {
		log.Debug("copying hash")
		copy(m.topHash, s)
	}

	// reset the chained writer
	m.putWriter(w)
	if job.edge > 0 && job.edge == m.targetLevel {
		log.Debug("reached target")
		return
	}

	// we only create a parent object on a job on the first write
	// this way, if it is nil and we are working the right edge, we know when to skip
	if p == nil {
		p = m.newJob(job.level + 1)
		job.parent = p
		log.Debug("set parent", "p", fmt.Sprintf("%p", p), "w", fmt.Sprintf("%p", p.writer))
	}
	if job.edge > 0 {
		p.edge = job.edge + 1
	}

	// write to the parent job
	// the section index to write to is divided by the branches
	//m.write(p, (index-1)/m.branches, s)
	m.write(p, index/m.branches, s)

}

// creates a new hasherJob
//func (m *FileSplitter) newJob(dataOffset uint64, levelOffset uint64, level int) *hasherJob {
//	return &hasherJob{
//		dataOffset:  dataOffset,
//		levelOffset: (levelOffset-1)/uint64(m.branches) + 1,
//		writer:      m.getWriter(),
//		level:       level,
//	}
//}
func (m *FileSplitter) newJob(level int) *altJob {
	return &altJob{
		writer: m.getWriter(),
		level:  level,
	}
}

// see writerMode consts
func (m *FileSplitter) getWriterPool() SectionHasherTwo {
	//m.writerQueue <- struct{}{}
	return m.writerPool.Get().(SectionHasherTwo)
}

// see writerMode consts
func (m *FileSplitter) putWriterPool(writer SectionHasherTwo) {
	writer.Reset()
	m.writerPool.Put(writer)
	//<-m.writerQueue
}

// resets a hasherJob for re-use.
// It will recursively reset parents as long as the respective levelOffets
// are on batch boundaries
//func (m *FileSplitter) reset(h *hasherJob, w SectionHasherTwo, dataOffset uint64, levelOffset uint64, edge int) {
//	h.debugLifetime++
//	h.count = 0
//	h.dataOffset = dataOffset
//	h.levelOffset = levelOffset
//	h.writer = w
//	if levelOffset%m.parentBatchSize == 0 && h.parent != nil {
//		m.reset(h.parent, m.getWriter(), dataOffset, levelOffset/m.writerBatchSize, edge+1)
//	}
//}
func (m *FileSplitter) reset(h *altJob, w SectionHasherTwo) {
	h.count = 0
	h.writer = w
}

// calculates if the given data write length results in a balanced tree
func (m *FileSplitter) isBalancedBoundary(count uint64) bool {
	_, ok := m.balancedTable[count]
	return ok
}

// utility structure for controlling asynchronous tree hashing of the file
type altJob struct {
	parent *altJob
	count  uint64 // amount of writes on this job
	edge   int    // > 0 on last write, incremented by 1 every level traversed on right edge, used to determine skipping levels on dangling chunk
	level  int
	writer SectionHasherTwo
}

func (h *altJob) inc() (uint64, uint64) {
	oldCount := atomic.LoadUint64(&h.count)
	newCount := atomic.AddUint64(&h.count, 1)
	return oldCount, newCount
}
