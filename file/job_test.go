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
	"github.com/ethersphere/swarm/log"
	"github.com/ethersphere/swarm/testutil"
	"golang.org/x/crypto/sha3"
)

var (
	dummyHashFunc = func() bmt.SectionWriter {
		return newDummySectionWriter(chunkSize*branches, sectionSize)
	}
	noHashFunc = func() bmt.SectionWriter {
		return nil
	}
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

	params := newTreeParams(sectionSize, branches, noHashFunc)

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

func TestTargetWithinJob(t *testing.T) {
	params := newTreeParams(sectionSize, branches, noHashFunc)
	params.Debug = true
	index := newJobIndex(9)

	jb := newJob(params, nil, index, 1, branches*branches)
	defer index.Delete(jb)

	finalSize := chunkSize*branches + chunkSize*2
	finalCount := dataSizeToSectionCount(finalSize, sectionSize)
	log.Trace("within test", "size", finalSize, "count", finalCount)
	c, ok := jb.targetWithinJob(finalCount - 1)
	if !ok {
		t.Fatalf("target %d within %d: expected true", finalCount, jb.level)
	}
	if c != 1 {
		t.Fatalf("target %d within %d: expected %d, got %d", finalCount, jb.level, 2, c)
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

func TestNewJob(t *testing.T) {

	params := newTreeParams(sectionSize, branches, noHashFunc)
	params.Debug = true

	tgt := newTarget()
	jb := newJob(params, tgt, nil, 1, branches*branches+1)

	if jb.level != 1 {
		t.Fatalf("job level expected 1, got %d", jb.level)
	}

	if jb.dataSection != branches*branches+1 {
		t.Fatalf("datasectionindex: expected %d, got %d", branches+1, jb.dataSection)
	}

}

func TestJobSize(t *testing.T) {

	params := newTreeParams(sectionSize, branches, noHashFunc)
	params.Debug = true
	index := newJobIndex(9)

	tgt := newTarget()
	jb := newJob(params, tgt, index, 3, 0)
	jb.cursorSection = 1
	jb.endCount = 1
	size := chunkSize*branches + chunkSize
	sections := dataSizeToSectionIndex(size, sectionSize) + 1
	tgt.Set(size, sections, 3)
	jobSize := jb.size()
	if jobSize != size {
		t.Fatalf("job size: expected %d, got %d", size, jobSize)
	}
	index.Delete(jb)

	tgt = newTarget()
	jb = newJob(params, tgt, index, 3, 0)
	jb.cursorSection = 1
	jb.endCount = 1
	size = chunkSize * branches * branches
	sections = dataSizeToSectionIndex(size, sectionSize) + 1
	tgt.Set(size, sections, 3)
	jobSize = jb.size()
	if jobSize != size {
		t.Fatalf("job size: expected %d, got %d", size, jobSize)
	}
	index.Delete(jb)

}

func TestJobTarget(t *testing.T) {

	tgt := newTarget()
	params := newTreeParams(sectionSize, branches, dummyHashFunc)
	params.Debug = true
	index := newJobIndex(9)

	startSection := branches * branches
	jb := newJob(params, tgt, index, 1, startSection) // second level index, equals 128 data chunk writes

	// anything less than chunksize * 128 will not be in the job span
	finalSize := chunkSize + sectionSize + 1
	finalSection := dataSizeToSectionIndex(finalSize, sectionSize)
	c, ok := jb.targetWithinJob(finalSection)
	if ok {
		t.Fatalf("targetwithinjob: expected false")
	}
	index.Delete(jb)

	// anything between chunksize*128 and chunksize*128*2 will be within the job span
	finalSize = chunkSize*branches + chunkSize*2
	finalSection = dataSizeToSectionIndex(finalSize, sectionSize)
	c, ok = jb.targetWithinJob(finalSection)
	if !ok {
		t.Fatalf("targetwithinjob section %d: expected true", startSection)
	}
	if c != 1 {
		t.Fatalf("targetwithinjob section %d: expected %d, got %d", startSection, 1, c)
	}
	c = jb.targetCountToEndCount(finalSection + 1)
	if c != 2 {
		t.Fatalf("targetcounttoendcount section %d: expected %d, got %d", startSection, 2, c)
	}
	index.Delete(jb)

}

func TestJobIndex(t *testing.T) {
	tgt := newTarget()
	params := newTreeParams(sectionSize, branches, dummyHashFunc)

	jb := newJob(params, tgt, nil, 1, branches)
	jobIndex := jb.index
	jbGot := jobIndex.Get(1, branches)
	if jb != jbGot {
		t.Fatalf("jbIndex get: expect %p, got %p", jb, jbGot)
	}
	jobIndex.Delete(jbGot)
	if jobIndex.Get(1, branches) != nil {
		t.Fatalf("jbIndex delete: expected nil")
	}
}

func TestGetJobNext(t *testing.T) {
	tgt := newTarget()
	params := newTreeParams(sectionSize, branches, dummyHashFunc)
	params.Debug = true

	jb := newJob(params, tgt, nil, 1, branches*branches)
	jbn := jb.Next()
	if jbn == nil {
		t.Fatalf("parent: nil")
	}
	if jbn.level != 1 {
		t.Fatalf("nextjob level: expected %d, got %d", 2, jbn.level)
	}
	if jbn.dataSection != jb.dataSection+branches*branches {
		t.Fatalf("nextjob section: expected %d, got %d", jb.dataSection+branches*branches, jbn.dataSection)
	}
}

func TestJobWriteTwoAndFinish(t *testing.T) {

	tgt := newTarget()
	params := newTreeParams(sectionSize*2, branches, dummyHashFunc)

	jb := newJob(params, tgt, nil, 1, 0)
	_, data := testutil.SerialData(sectionSize*2, 255, 0)
	jb.write(0, data[:sectionSize])
	jb.write(1, data[:sectionSize])

	finalSize := chunkSize * 2
	finalSection := dataSizeToSectionIndex(finalSize, sectionSize)
	tgt.Set(finalSize, finalSection, 2)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*199)
	defer cancel()
	select {
	case <-tgt.Done():
	case <-ctx.Done():
		t.Fatalf("timeout: %v", ctx.Err())
	}

	if jb.count() != 2 {
		t.Fatalf("jobcount: expected %d, got %d", 2, jb.count())
	}
}

func TestGetJobParent(t *testing.T) {
	tgt := newTarget()
	params := newTreeParams(sectionSize, branches, dummyHashFunc)

	jb := newJob(params, tgt, nil, 1, branches*branches)
	jbp := jb.parent()
	if jbp == nil {
		t.Fatalf("parent: nil")
	}
	if jbp.level != 2 {
		t.Fatalf("parent level: expected %d, got %d", 2, jbp.level)
	}
	if jbp.dataSection != 0 {
		t.Fatalf("parent data section: expected %d, got %d", 0, jbp.dataSection)
	}
	jbGot := jb.index.Get(2, 0)
	if jbGot == nil {
		t.Fatalf("index get: nil")
	}

	jbNext := jb.Next()
	jbpNext := jbNext.parent()
	if jbpNext != jbp {
		t.Fatalf("next parent: expected %p, got %p", jbp, jbpNext)
	}
}

func TestWriteParentSection(t *testing.T) {
	tgt := newTarget()
	params := newTreeParams(sectionSize, branches, dummyHashFunc)
	index := newJobIndex(9)

	jb := newJob(params, tgt, index, 1, 0)
	jbn := jb.Next()
	_, data := testutil.SerialData(sectionSize*2, 255, 0)
	jbn.write(0, data[:sectionSize])
	jbn.write(1, data[sectionSize:])

	finalSize := chunkSize*branches + chunkSize*2
	finalSection := dataSizeToSectionIndex(finalSize, sectionSize)
	tgt.Set(finalSize, finalSection, 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	select {
	case <-tgt.Done():
		t.Fatalf("unexpected done")
	case <-ctx.Done():
	}
	jbnp := jbn.parent()
	if jbnp.count() != 1 {
		t.Fatalf("parent count: expected %d, got %d", 1, jbnp.count())
	}
	correctRefHex := "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
	parentRef := jbnp.writer.(*dummySectionWriter).data[32:64]
	parentRefHex := hexutil.Encode(parentRef)
	if parentRefHex != correctRefHex {
		t.Fatalf("parent data: expected %s, got %s", correctRefHex, parentRefHex)
	}
}

func TestJobWriteFull(t *testing.T) {

	tgt := newTarget()
	params := newTreeParams(sectionSize, branches, dummyHashFunc)

	jb := newJob(params, tgt, nil, 1, 0)

	_, data := testutil.SerialData(chunkSize, 255, 0)
	for i := 0; i < branches; i++ {
		jb.write(i, data[i*sectionSize:i*sectionSize+sectionSize])
	}

	tgt.Set(chunkSize, branches, 2)
	correctRefHash := "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	select {
	case ref := <-tgt.Done():
		refHash := hexutil.Encode(ref)
		if refHash != correctRefHash {
			t.Fatalf("job write full: expected %s, got %s", correctRefHash, refHash)
		}
	case <-ctx.Done():
		t.Fatalf("timeout: %v", ctx.Err())
	}
	if jb.count() != branches {
		t.Fatalf("jobcount: expected %d, got %d", 32, jb.count())
	}
}

func TestJobWriteSpan(t *testing.T) {

	tgt := newTarget()
	pool := bmt.NewTreePool(sha3.NewLegacyKeccak256, branches, bmt.PoolSize)
	hashFunc := func() bmt.SectionWriter {
		return bmt.New(pool).NewAsyncWriter(false)
	}
	params := newTreeParams(sectionSize, branches, hashFunc)

	jb := newJob(params, tgt, nil, 1, 0)
	_, data := testutil.SerialData(chunkSize+sectionSize*2, 255, 0)

	for i := 0; i < chunkSize; i += sectionSize {
		jb.write(i/sectionSize, data[i:i+sectionSize])
	}
	jbn := jb.Next()
	jbn.write(0, data[chunkSize:chunkSize+sectionSize])
	jbn.write(1, data[chunkSize+sectionSize:])
	finalSize := chunkSize*branches + chunkSize*2
	finalSection := dataSizeToSectionIndex(finalSize, sectionSize)
	tgt.Set(finalSize, finalSection, 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
	defer cancel()
	select {
	case ref := <-tgt.Done():
		// TODO: double check that this hash if correct!!
		refCorrectHex := "0xee56134cab34a5a612648dcc22d88b7cb543081bd144906dfc4fa93802c9addf"
		refHex := hexutil.Encode(ref)
		if refHex != refCorrectHex {
			t.Fatalf("writespan sequential: expected %s, got %s", refCorrectHex, refHex)
		}
	case <-ctx.Done():
		t.Fatalf("timeout: %v", ctx.Err())
	}

	sz := jb.size()
	if sz != chunkSize*branches {
		t.Fatalf("job 1 size: expected %d, got %d", chunkSize, sz)
	}

	sz = jbn.size()
	if sz != chunkSize*2 {
		t.Fatalf("job 2 size: expected %d, got %d", sectionSize, sz)
	}
}

func TestJobWriteSpanShuffle(t *testing.T) {

	tgt := newTarget()
	pool := bmt.NewTreePool(sha3.NewLegacyKeccak256, branches, bmt.PoolSize)
	hashFunc := func() bmt.SectionWriter {
		return bmt.New(pool).NewAsyncWriter(false)
	}
	params := newTreeParams(sectionSize, branches, hashFunc)

	jb := newJob(params, tgt, nil, 1, 0)
	_, data := testutil.SerialData(chunkSize+sectionSize*2, 255, 0)

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

	jbn := jb.Next()
	jbn.write(0, data[chunkSize:chunkSize+sectionSize])
	jbn.write(1, data[chunkSize+sectionSize:])
	finalSize := chunkSize*branches + chunkSize*2
	finalSection := dataSizeToSectionIndex(finalSize, sectionSize)
	tgt.Set(finalSize, finalSection, 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	select {
	case ref := <-tgt.Done():
		refCorrectHex := "0xee56134cab34a5a612648dcc22d88b7cb543081bd144906dfc4fa93802c9addf"
		refHex := hexutil.Encode(ref)
		jbparent := jb.parent()
		jbnparent := jbn.parent()
		log.Info("succeeding", "jb count", jb.count(), "jbn count", jbn.count(), "jb parent count", jbparent.count(), "jbn parent count", jbnparent.count())
		if refHex != refCorrectHex {
			t.Fatalf("writespan sequential: expected %s, got %s", refCorrectHex, refHex)
		}
	case <-ctx.Done():

		jbparent := jb.parent()
		jbnparent := jbn.parent()
		log.Error("failing", "jb count", jb.count(), "jbn count", jbn.count(), "jb parent count", jbparent.count(), "jbn parent count", jbnparent.count())
		t.Fatalf("timeout: %v", ctx.Err())
	}

	sz := jb.size()
	if sz != chunkSize*branches {
		t.Fatalf("job size: expected %d, got %d", chunkSize*branches, sz)
	}

	sz = jbn.size()
	if sz != chunkSize*2 {
		t.Fatalf("job size: expected %d, got %d", chunkSize*branches, sz)
	}
}

// TestVectors executes the barebones functionality of the hasher
// TODO: vet against the referencefilehasher instead of expect vector
func TestVectors(t *testing.T) {
	poolAsync := bmt.NewTreePool(sha3.NewLegacyKeccak256, branches, bmt.PoolSize)
	poolSync := bmt.NewTreePool(sha3.NewLegacyKeccak256, branches, bmt.PoolSize)
	refHashFunc := func() bmt.SectionWriter {
		return bmt.New(poolAsync).NewAsyncWriter(false)
	}
	dataHash := bmt.New(poolSync)
	params := newTreeParams(sectionSize, branches, refHashFunc)

	for i := start; i < end; i++ {
		tgt := newTarget()
		index := newJobIndex(9)
		dataLength := dataLengths[i]
		_, data := testutil.SerialData(dataLength, 255, 0)
		jb := newJob(params, tgt, nil, 1, 0)
		count := 0
		log.Info("test vector", "length", dataLength)
		for i := 0; i < dataLength; i += chunkSize {
			ie := i + chunkSize
			if ie > dataLength {
				ie = dataLength
			}
			writeSize := ie - i
			span := lengthToSpan(writeSize)
			log.Debug("data write", "i", i, "length", writeSize, "span", span)
			dataHash.ResetWithLength(span)
			c, err := dataHash.Write(data[i:ie])
			if err != nil {
				index.Delete(jb)
				t.Fatalf("data ref fail: %v", err)
			}
			if c != ie-i {
				index.Delete(jb)
				t.Fatalf("data ref short write: expect %d, got %d", ie-i, c)
			}
			ref := dataHash.Sum(nil)
			log.Debug("data ref", "i", i, "ie", ie, "data", hexutil.Encode(ref))
			jb.write(count, ref)
			count += 1
			if ie%(chunkSize*branches) == 0 {
				jb = jb.Next()
				count = 0
			}
		}
		dataSections := dataSizeToSectionIndex(dataLength, params.SectionSize)
		tgt.Set(dataLength, dataSections, getLevelsFromLength(dataLength, params.SectionSize, params.Branches))
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
		defer cancel()
		select {
		case ref := <-tgt.Done():
			// TODO: double check that this hash if correct!!
			refCorrectHex := "0x" + expected[i]
			refHex := hexutil.Encode(ref)
			if refHex != refCorrectHex {
				t.Fatalf("writespan sequential: expected %s, got %s", refCorrectHex, refHex)
			}
		case <-ctx.Done():
			t.Fatalf("timeout: %v", ctx.Err())
		}
	}
}
