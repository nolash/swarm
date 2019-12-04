package hasher

import (
	"context"
	"sync"

	"github.com/ethersphere/swarm/bmt"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/param"
)

type BMTSyncSectionWriter struct {
	hasher *bmt.Hasher
	data   []byte
}

func NewBMTSyncSectionWriter(hasher *bmt.Hasher) param.SectionWriter {
	return &BMTSyncSectionWriter{
		hasher: hasher,
	}
}

func (b *BMTSyncSectionWriter) Init(_ context.Context, errFunc func(error)) {
}

func (b *BMTSyncSectionWriter) Link(_ func() param.SectionWriter) {
}

func (b *BMTSyncSectionWriter) Sum(extra []byte, _ int, span []byte) []byte {
	b.hasher.ResetWithLength(span)
	b.hasher.Write(b.data)
	return b.hasher.Sum(extra)
}

func (b *BMTSyncSectionWriter) Reset(_ context.Context) {
	b.hasher.Reset()
}

func (b *BMTSyncSectionWriter) Write(_ int, data []byte) {
	b.data = data
}

func (b *BMTSyncSectionWriter) SectionSize() int {
	return b.hasher.ChunkSize()
}

func (b *BMTSyncSectionWriter) DigestSize() int {
	return b.hasher.Size()
}

// Hasher is a bmt.SectionWriter that executes the file hashing algorithm on arbitary data
type Hasher struct {
	target *target
	params *treeParams
	index  *jobIndex

	job        *job // current level 1 job being written to
	writerPool sync.Pool
	hasherPool sync.Pool
	size       int
	count      int
}

// New creates a new Hasher object using the given sectionSize and branch factor
// hasherFunc is used to create *bmt.Hashers to hash the incoming data
// writerFunc is used as the underlying bmt.SectionWriter for the asynchronous hasher jobs. It may be pipelined to other components with the same interface
func New(sectionSize int, branches int, hasherFunc func() param.SectionWriter) *Hasher {
	h := &Hasher{
		target: newTarget(),
		index:  newJobIndex(9),
	}
	h.params = newTreeParams(sectionSize, branches, h.getWriter)
	h.writerPool.New = func() interface{} {
		return h.params.hashFunc()
	}
	h.hasherPool.New = func() interface{} {
		return hasherFunc()
	}
	h.job = newJob(h.params, h.target, h.index, 1, 0)
	return h
}

func (h *Hasher) Init(ctx context.Context, errFunc func(error)) {
	h.params.SetContext(ctx)
}

func (h *Hasher) Link(writerFunc func() param.SectionWriter) {
	h.params.hashFunc = writerFunc
	h.job.start()
}

// Write implements bmt.SectionWriter
// It as a non-blocking call that hashes a data chunk and passes the resulting reference to the hash job representing
// the intermediate chunk holding the data references
// TODO: enforce buffered writes and limits
// TODO: attempt omit modulo calc on every pass
// TODO: preallocate full size span slice
func (h *Hasher) Write(index int, b []byte) {
	if h.count%h.params.Branches == 0 && h.count > 0 {
		h.job = h.job.Next()
	}
	go func(i int, jb *job) {
		hasher := h.getHasher(len(b))
		hasher.Write(0, b)
		l := len(b)
		span := bmt.LengthToSpan(l)
		ref := hasher.Sum(nil, l, span)
		chunk.NewChunk(ref, append(span, b...))
		jb.write(i%h.params.Branches, ref)
		h.putHasher(hasher)
	}(h.count, h.job)
	h.size += len(b)
	h.count++
}

// Sum implements bmt.SectionWriter
// It is a blocking call that calculates the target level and section index of the received data
// and alerts hasher jobs the end of write is reached
// It returns the root hash
func (h *Hasher) Sum(_ []byte, length int, _ []byte) []byte {
	sectionCount := dataSizeToSectionIndex(h.size, h.params.SectionSize)
	targetLevel := getLevelsFromLength(h.size, h.params.SectionSize, h.params.Branches)
	h.target.Set(h.size, sectionCount, targetLevel)
	return <-h.target.Done()
}

func (h *Hasher) Reset(ctx context.Context) {
	h.params.ctx = ctx
}

func (h *Hasher) SectionSize() int {
	return h.params.ChunkSize
}

func (h *Hasher) DigestSize() int {
	return h.params.SectionSize
}

// proxy for sync.Pool
func (h *Hasher) putHasher(w param.SectionWriter) {
	h.hasherPool.Put(w)
}

// proxy for sync.Pool
func (h *Hasher) getHasher(l int) param.SectionWriter {
	//span := bmt.LengthToSpan(l)
	hasher := h.hasherPool.Get().(param.SectionWriter)
	hasher.Reset(h.params.ctx) //WithLength(span)
	return hasher
}

// proxy for sync.Pool
func (h *Hasher) putWriter(w param.SectionWriter) {
	w.Reset(h.params.ctx)
	h.writerPool.Put(w)
}

// proxy for sync.Pool
func (h *Hasher) getWriter() param.SectionWriter {
	return h.writerPool.Get().(param.SectionWriter)
}