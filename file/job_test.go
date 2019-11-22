package file

import (
	"bytes"
	"context"
	"hash"
	"math/rand"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethersphere/swarm/bmt"
	"github.com/ethersphere/swarm/testutil"
	"golang.org/x/crypto/sha3"
)

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
func TestTreeParams(t *testing.T) {

	params := newTreeParams(sectionSize, branches)

	if params.SectionSize != 32 {
		t.Fatalf("section: expected %d, got %d", sectionSize, params.SectionSize)
	}

	if params.Branches != 128 {
		t.Fatalf("branches: expected %d, got %d", branches, params.SectionSize)
	}

	if params.Spans[2] != branches*branches {
		t.Fatalf("span %d: expected %d, got %d", 2, branches*branches, params.Spans[1])
	}

}

// BUG: leaks goroutine
func TestTarget(t *testing.T) {

	tgt := newTarget()
	tgt.Set(32, 1, 2)

	if tgt.size != 32 {
		t.Fatalf("target size expected %d, got %d", 32, tgt.size)
	}

	if tgt.sections != 1 {
		t.Fatalf("target sections expected %d, got %d", 1, tgt.sections)
	}

	if tgt.level != 2 {
		t.Fatalf("target level expected %d, got %d", 2, tgt.level)
	}
}

// BUG: leaks goroutine
func TestNewJob(t *testing.T) {

	tgt := newTarget()
	params := newTreeParams(sectionSize, branches)
	writer := newDummySectionWriter(chunkSize*2, sectionSize)

	jb := newJob(params, tgt, writer, 1, branches+1)

	if jb.level != 1 {
		t.Fatalf("job level expected 1, got %d", jb.level)
	}

	if jb.dataSection != branches+1 {
		t.Fatalf("datasectionindex: expected %d, got %d", branches+1, jb.dataSection)
	}

	if jb.levelSection != 1 {
		t.Fatalf("levelsectionindex: expected %d, got %d", 1, jb.levelSection)
	}

}

func TestJobTarget(t *testing.T) {

	tgt := newTarget()
	params := newTreeParams(sectionSize, branches)
	writer := newDummySectionWriter(chunkSize*2, sectionSize)

	jb := newJob(params, tgt, writer, 1, branches*branches) // second level index, equals 128 data chunk writes

	// anything less than chunksize * 128 will not be in the job span
	finalSize := chunkSize + sectionSize + 1
	finalSection := dataSizeToSectionIndex(finalSize, sectionSize)
	_, ok := jb.targetWithinJob(finalSection)
	if ok {
		t.Fatalf("targetwithinjob: expected false")
	}

	// anything between chunksize*128 and chunksize*128*2 will be within the job span
	finalSize = chunkSize*branches + chunkSize*2
	finalSection = dataSizeToSectionIndex(finalSize, sectionSize)
	c, ok := jb.targetWithinJob(finalSection)
	if !ok {
		t.Fatalf("targetwithinjob: expected true")
	}
	if c != 1 {
		t.Fatalf("targetwithinjob: expected %d, got %d", 1, c)
	}

	_, data := testutil.SerialData(sectionSize*2-1, 255, 0)
	jb.write(0, data[:sectionSize])
	jb.write(1, data[sectionSize:])

	tgt.Set(finalSize, finalSection, 2)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	select {
	case <-tgt.Done():
	case <-ctx.Done():
		t.Fatalf("timeout: %v", ctx.Err())
	}

}

func TestJobWriteOneAndFinish(t *testing.T) {

	tgt := newTarget()
	params := newTreeParams(sectionSize, branches)
	writer := newDummySectionWriter(chunkSize*2, sectionSize)

	jb := newJob(params, tgt, writer, 1, branches)
	_, data := testutil.SerialData(32, 255, 0)
	jb.write(1, data)

	if jb.count() != 1 {
		t.Fatalf("jobcount: expected %d, got %d", 1, jb.count())
	}

	tgt.Set(32, 1, 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	select {
	case <-tgt.Done():
	case <-ctx.Done():
		t.Fatalf("timeout: %v", ctx.Err())
	}
}

func TestJobWriteFull(t *testing.T) {

	tgt := newTarget()
	params := newTreeParams(sectionSize, branches)
	writer := newDummySectionWriter(chunkSize*2, sectionSize)

	jb := newJob(params, tgt, writer, 1, branches)

	_, data := testutil.SerialData(chunkSize, 255, 0)
	for i := 0; i < branches; i++ {
		jb.write(i, data[i*sectionSize:i*sectionSize+sectionSize])
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	select {
	case <-tgt.Done():
	case <-ctx.Done():
		t.Fatalf("timeout: %v", ctx.Err())
	}
	if jb.count() != branches {
		t.Fatalf("jobcount: expected %d, got %d", 32, jb.count())
	}
}

func TestJobWriteSpan(t *testing.T) {

	tgt := newTarget()
	params := newTreeParams(sectionSize, branches)
	pool := bmt.NewTreePool(sha3.NewLegacyKeccak256, branches, bmt.PoolSize)
	writer := bmt.New(pool).NewAsyncWriter(false)

	jb := newJob(params, tgt, writer, 1, branches)
	_, data := testutil.SerialData(chunkSize, 255, 0)

	for i := 0; i < len(data); i += sectionSize {
		jb.write(i/sectionSize, data[i:i+sectionSize])
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	select {
	case ref := <-tgt.Done():
		t.Logf("%x", ref)
	case <-ctx.Done():
		t.Fatalf("timeout: %v", ctx.Err())
	}

	sz := jb.size()
	if sz != chunkSize*branches {
		t.Fatalf("job size: expected %d, got %d", chunkSize*branches, sz)
	}
}

func TestJobWriteSpanShuffle(t *testing.T) {

	tgt := newTarget()
	params := newTreeParams(sectionSize, branches)
	pool := bmt.NewTreePool(sha3.NewLegacyKeccak256, branches, bmt.PoolSize)
	writer := bmt.New(pool).NewAsyncWriter(false)

	jb := newJob(params, tgt, writer, 1, branches)
	_, data := testutil.SerialData(chunkSize, 255, 0)

	var idxs []int
	for i := 0; i < branches; i++ {
		idxs = append(idxs, i)
	}
	rand.Shuffle(branches, func(i int, j int) {
		idxs[i], idxs[j] = idxs[j], idxs[i]
	})
	for _, idx := range idxs {
		jb.write(idx, data[idx*sectionSize:idx*sectionSize+sectionSize])
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
	defer cancel()
	select {
	case ref := <-tgt.Done():
		t.Logf("%x", ref)
	case <-ctx.Done():
		t.Fatalf("timeout: %v", ctx.Err())
	}

	sz := jb.size()
	if sz != chunkSize*branches {
		t.Fatalf("job size: expected %d, got %d", chunkSize*branches, sz)
	}
}
