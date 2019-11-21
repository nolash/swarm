package storage

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/ethersphere/swarm/bmt"
	"github.com/ethersphere/swarm/log"
	"golang.org/x/crypto/sha3"
)

func newTestSplitter(hasherFunc func() bmt.SectionWriter) (*FileSplitterTwo, error) {
	hashFunc := func() SectionHasherTwo {
		return &wrappedHasher{
			SectionWriter: hasherFunc(),
		}
	}
	dataHasher := newSyncHasher()
	return NewFileSplitterTwo(dataHasher, hashFunc)
}

// mock writer for testing and debugging
// implements SectionHasherTwo
type testFileWriter struct {
	count       uint64
	sectionSize uint64
	data        []byte
}

func newTestFileWriter(cp int, sectionSize uint64) *testFileWriter {
	return &testFileWriter{
		sectionSize: sectionSize,
		data:        make([]byte, cp),
	}
}

func (t *testFileWriter) SectionSize() int {
	return int(t.sectionSize)
}

func (t *testFileWriter) Write(index int, b []byte) {
	copy(t.data[uint64(index)*t.sectionSize:], b)
	atomic.AddUint64(&t.count, sectionCount(b, t.sectionSize))
}

func (t *testFileWriter) Sum(b []byte, length int, span []byte) []byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(t.data)
	s := h.Sum(nil)
	h.Reset()
	return s
}

func (t *testFileWriter) Reset() {
	return
}

func TestFileSplitterParentIndex(t *testing.T) {
	fh, err := newTestSplitter(newAsyncHasher)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("filehasher set up", "batchsize", fh.BatchSize(), "padsize", fh.PadSize())
	vals := []uint64{31, 32, 33, 127, 128, 129, 128*128 - 1, 128 * 128, 128*128 + 1}
	expects := []uint64{0, 0, 0, 0, 1, 1, 127, 128, 128}
	for i, v := range vals {
		idx := fh.getParentSection(v)
		if idx != expects[i] {
			t.Fatalf("parent index %d expected %d, got %d", v, expects[i], idx)
		}
	}
}

func TestFileSplitterCreateJob(t *testing.T) {
	fh, err := newTestSplitter(newAsyncHasher)
	if err != nil {
		t.Fatal(err)
	}

	idx := fh.chunkSize * fh.chunkSize
	job := fh.newHashJobTwo(1, idx, idx)
	if job.parentSection != idx/fh.branches {
		t.Fatalf("Expected parent parentSection %d, got %d", idx/fh.branches, job.parentSection)
	}
	parent := fh.getOrCreateParent(job)
	if parent.level != 2 {
		t.Fatalf("Expected parent level 2, got %d", parent.level)
	}
	if parent.firstDataSection != idx {
		t.Fatalf("Expected parent dataIndex %d, got %d", idx, parent.parentSection)
	}
	if parent.parentSection != job.parentSection/fh.branches {
		t.Fatalf("Expected parent parentSection %d, got %d", job.parentSection/fh.branches, parent.parentSection)
	}
}

func TestFileSplitterWriteJob(t *testing.T) {
	w := newTestFileWriter(chunkSize, segmentSize)
	fh, err := newTestSplitter(func() bmt.SectionWriter {
		return w
	})
	if err != nil {
		t.Fatal(err)
	}

	// filesplitter creates the first level 1 job for dataindex 0 automatically
	job := fh.lastJob
	jobIndexed, ok := fh.levels[1].jobs[0]
	if !ok {
		t.Fatalf("expected level 1 idx 0 job to be in level index")
	}
	if job != jobIndexed {
		t.Fatalf("expected job in index to match initial job")
	}
	data := make([]byte, segmentSize)
	rand.Seed(23115) // arbitrary value
	c, err := rand.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	if c != segmentSize {
		t.Fatalf("short rand read %d", c)
	}
	hasher := sha3.NewLegacyKeccak256()
	job.write(4, data)
	if !bytes.Equal(w.data[segmentSize*4:segmentSize*4+segmentSize], data) {
		t.Fatalf("data mismatch in writer at pos %d", segmentSize*2)
	}

	hasher.Write(w.data)
	ref := hasher.Sum(nil)
	hasher.Reset()
	fh.sum(job)
	if !bytes.Equal(w.data[:segmentSize], ref) {
		t.Fatalf("hash result mismatch in writer after first sum, expected %x, got %x", ref, w.data[:segmentSize])
	}

	_, ok = fh.levels[1].jobs[0]
	if ok {
		t.Fatalf("expected level 1 idx 0 job to be deleted")
	}

	job = fh.newHashJobTwo(1, fh.branches, fh.branches)
	job.write(2, data)
	if !bytes.Equal(w.data[segmentSize*2:segmentSize*2+segmentSize], data) {
		t.Fatalf("data mismatch in writer at pos %d", segmentSize*2)
	}
	hasher.Write(w.data)
	ref = hasher.Sum(nil)
	hasher.Reset()
	fh.sum(job)
	if !bytes.Equal(w.data[segmentSize:segmentSize*2], ref) {
		t.Fatalf("hash result mismatch in writer after second sum")
	}

}
