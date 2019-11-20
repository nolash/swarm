package storage

import (
	"testing"

	"github.com/ethersphere/swarm/log"
)

func newTestSplitter() (*FileSplitterTwo, error) {
	hashFunc := func() SectionHasherTwo {
		return &wrappedHasher{
			SectionWriter: newAsyncHasher(),
		}
	}
	dataHasher := newSyncHasher()
	return NewFileSplitterTwo(dataHasher, hashFunc)
}

func TestFileSplitterParentIndex(t *testing.T) {
	fh, err := newTestSplitter()
	if err != nil {
		t.Fatal(err)
	}
	log.Info("filehasher set up", "batchsize", fh.BatchSize(), "padsize", fh.PadSize())
	vals := []uint64{31, 32, 33, 127, 128, 129, 128*128 - 1, 128 * 128, 128*128 + 1}
	expects := []uint64{0, 0, 0, 0, 1, 1, 127, 128, 128}
	for i, v := range vals {
		idx := fh.getParentIndex(v)
		if idx != expects[i] {
			t.Fatalf("parent index %d expected %d, got %d", v, expects[i], idx)
		}
	}
}

func TestFileSplitterCreateJob(t *testing.T) {
	fh, err := newTestSplitter()
	if err != nil {
		t.Fatal(err)
	}
	job := fh.newHashJobTwo(1, 0, 0)
	parent := fh.getOrCreateParent(job)
	if parent.level != 2 {
		t.Fatalf("Expected parent level 2, got %d", parent.level)
	}

	idx := fh.chunkSize * fh.chunkSize
	job = fh.newHashJobTwo(1, idx, idx)
	if job.parentIndex != idx/fh.branches {
		t.Fatalf("Expected parent parentIndex %d, got %d", idx/fh.branches, job.parentIndex)
	}
	parent = fh.getOrCreateParent(job)
	if parent.level != 2 {
		t.Fatalf("Expected parent level 2, got %d", parent.level)
	}
	if parent.dataIndex != idx {
		t.Fatalf("Expected parent dataIndex %d, got %d", idx, parent.parentIndex)
	}
	if parent.parentIndex != job.parentIndex/fh.branches {
		t.Fatalf("Expected parent parentIndex %d, got %d", job.parentIndex/fh.branches, parent.parentIndex)
	}
}
