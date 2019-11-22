package file

import (
	"bytes"
	"hash"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"golang.org/x/crypto/sha3"
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
	params := newTreeParams(sectionSize, branches)
	writer := newDummySectionWriter(chunkSize*2, sectionSize)

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

func TestJobWrite(t *testing.T) {

	var tgt target
	params := newTreeParams(sectionSize, branches)
	writer := newDummySectionWriter(chunkSize*2, sectionSize)

	job := newJob(params, &tgt, writer, 1, branches)

	data := make([]byte, sectionSize)
	job.write(1, data)

	if job.count != 1 {
		t.Fatalf("jobcount: expected %d, got %d", 1, job.count)
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
		writer:      sha3.NewLegacyKeccak256(),
	}
}

func (d *dummySectionWriter) Write(index int, data []byte) {
	copy(d.data[index*sectionSize:], data)
}

func (d *dummySectionWriter) Sum(b []byte, size int, span []byte) []byte {
	return d.writer.Sum(b)
}

func (d *dummySectionWriter) Reset() {
	d.data = make([]byte, len(d.data))
	d.writer.Reset()
}

func (d *dummySectionWriter) SectionSize() int {
	return d.sectionSize
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
	if !bytes.Equal(w.data[chunkSize:chunkSize+32], data) {
		t.Fatalf("Write pos %d: expected %x, got %x", chunkSize, w.data[chunkSize:chunkSize+32], data)
	}

	correctDigest := "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
	digest := w.Sum(nil, chunkSize*2, nil)
	if hexutil.Encode(digest) != correctDigest {
		t.Fatalf("Digest: expected %s, got %x", correctDigest, digest)
	}
}
