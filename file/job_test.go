package file

import (
	"crypto/sha1"
	"hash"
	"math/rand"
	"testing"

	"github.com/ethersphere/swarm/bmt"
)

func TestTreeParams(t *testing.T) {

	params := newTreeParams(sectionSize, branches)

	if params.section != 32 {
		t.Fatalf("section: expected %d, got %d", sectionSize, params.section)
	}

	if params.branches != 128 {
		t.Fatalf("branches: expected %d, got %d", branches, params.section)
	}

	if params.spans[2] != branches*branches {
		t.Fatalf("span %d: expected %d, got %d", 2, branches*branches, params.spans[1])
	}

}

func TestNewJob(t *testing.T) {

	var tgt *target
	var params *treeParams
	var writer bmt.SectionWriter

	params = newTreeParams(sectionSize, branches)

	job := newJob(params, tgt, writer, 1, branches+1)

	if job.level != 1 {
		t.Fatalf("job level expected 1, got %d", job.level)
	}

	if job.dataSection != branches+1 {
		t.Fatalf("datasectionindex: expected %d, got %d", branches+1, job.dataSection)
	}

	if job.levelSection != 1 {
		t.Fatalf("levelsectionindex: expected %d, got %d", 1, job.levelSection)
	}

}

type dummySectionWriter struct {
	data        []byte
	sectionSize int
	writer      hash.Hash
}

func newDummySectionWriter(cp int, sectionSize int) *dummySectionWriter {
	return &dummySectionWriter{
		data:        make([]byte, cp),
		sectionSize: sectionSize,
		writer:      sha1.New(),
	}
}

func (d *dummySectionWriter) Write(index int, data []byte) {
	copy(d.data[index*sectionSize:], data)
}

func (d *dummySectionWriter) Reset() {
	d.data = make([]byte, len(d.data))
}

func TestDummySectionWriter(t *testing.T) {

	w := newDummySectionWriter(chunkSize*2, sectionSize)
	w.Reset()

	data := make([]byte, 32)
	rand.Seed(23115)
	c, err := rand.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	if c < 32 {
		t.Fatalf("short read %d", c)
	}

	w.Write(branches, data)
}
