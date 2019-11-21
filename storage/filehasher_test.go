package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/crypto/sha3"

	"github.com/ethersphere/swarm/bmt"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/log"
)

const (
	segmentSize = 32
	branches    = 128
	chunkSize   = 4096
)

var pool *bmt.TreePool

var (
	dataLengths = []int{31, // 0
		32,                     // 1
		33,                     // 2
		63,                     // 3
		64,                     // 4
		65,                     // 5
		chunkSize,              // 6
		chunkSize + 31,         // 7
		chunkSize + 32,         // 8
		chunkSize + 63,         // 9
		chunkSize + 64,         // 10
		chunkSize * 2,          // 11
		chunkSize*2 + 32,       // 12
		chunkSize * 128,        // 13
		chunkSize*128 + 31,     // 14
		chunkSize*128 + 32,     // 15
		chunkSize*128 + 64,     // 16
		chunkSize * 129,        // 17
		chunkSize * 130,        // 18
		chunkSize*128*128 - 32, // 19
	}
	expected = []string{
		"ece86edb20669cc60d142789d464d57bdf5e33cb789d443f608cbd81cfa5697d", // 0
		"0be77f0bb7abc9cd0abed640ee29849a3072ccfd1020019fe03658c38f087e02", // 1
		"3463b46d4f9d5bfcbf9a23224d635e51896c1daef7d225b86679db17c5fd868e", // 2
		"95510c2ff18276ed94be2160aed4e69c9116573b6f69faaeed1b426fea6a3db8", // 3
		"490072cc55b8ad381335ff882ac51303cc069cbcb8d8d3f7aa152d9c617829fe", // 4
		"541552bae05e9a63a6cb561f69edf36ffe073e441667dbf7a0e9a3864bb744ea", // 5
		"c10090961e7682a10890c334d759a28426647141213abda93b096b892824d2ef", // 6
		"91699c83ed93a1f87e326a29ccd8cc775323f9e7260035a5f014c975c5f3cd28", // 7
		"73759673a52c1f1707cbb61337645f4fcbd209cdc53d7e2cedaaa9f44df61285", // 8
		"db1313a727ffc184ae52a70012fbbf7235f551b9f2d2da04bf476abe42a3cb42", // 9
		"ade7af36ac0c7297dc1c11fd7b46981b629c6077bce75300f85b02a6153f161b", // 10
		"29a5fb121ce96194ba8b7b823a1f9c6af87e1791f824940a53b5a7efe3f790d9", // 11
		"61416726988f77b874435bdd89a419edc3861111884fd60e8adf54e2f299efd6", // 12
		"3047d841077898c26bbe6be652a2ec590a5d9bd7cd45d290ea42511b48753c09", // 13
		"e5c76afa931e33ac94bce2e754b1bb6407d07f738f67856783d93934ca8fc576", // 14
		"485a526fc74c8a344c43a4545a5987d17af9ab401c0ef1ef63aefcc5c2c086df", // 15
		"624b2abb7aefc0978f891b2a56b665513480e5dc195b4a66cd8def074a6d2e94", // 16
		"b8e1804e37a064d28d161ab5f256cc482b1423d5cd0a6b30fde7b0f51ece9199", // 17
		"59de730bf6c67a941f3b2ffa2f920acfaa1713695ad5deea12b4a121e5f23fa1", // 18
		"522194562123473dcfd7a457b18ee7dee8b7db70ed3cfa2b73f348a992fdfd3b", // 19
	}

	start = 0
	end   = 13 //len(dataLengths)
)

type wrappedHasher struct {
	bmt.SectionWriter
}

func newAsyncHasher() bmt.SectionWriter {
	h := newSyncHasher()
	return h.NewAsyncWriter(false)
}

func newSyncHasher() *bmt.Hasher {
	pool = bmt.NewTreePool(sha3.NewLegacyKeccak256, 128, bmt.PoolSize)
	h := bmt.New(pool)
	return h
}

func (w *wrappedHasher) BatchSize() uint64 {
	return 128
}

func (w *wrappedHasher) PadSize() uint64 {
	return 0
}

func TestChainedFileHasher(t *testing.T) {
	hashFunc := func() SectionHasherTwo {
		return &wrappedHasher{
			SectionWriter: newAsyncHasher(),
		}
	}

	for i := start; i < end; i++ {
		dataHasher := newSyncHasher()
		//		fh, err := NewFileSplitter(dataHasher, hashFunc, writerModePool)
		//		if err != nil {
		//			t.Fatal(err)
		//		}
		fh, err := NewFileSplitterTwo(dataHasher, hashFunc)
		if err != nil {
			t.Fatal(err)
		}
		log.Info("filehasher set up", "batchsize", fh.BatchSize(), "padsize", fh.PadSize())

		dataLength := dataLengths[i]
		_, data := generateSerialData(dataLength, 255, 0)
		log.Info(">>>>>>>>> NewFileHasher start", "i", i, "len", dataLength)
		offset := 0
		l := 4096
		for j := 0; j < dataLength; j += 4096 {
			remain := dataLength - offset
			if remain < l {
				l = remain
			}
			fh.Write(int(offset/32), data[offset:offset+l])
			offset += 4096
		}
		refHash := fh.Sum(nil, 0, nil) //span)
		t.Logf("Final (%02d): %x (expected %s)", i, refHash, expected[i])
	}
}

func BenchmarkChainedFileHasher(b *testing.B) {
	for i := start; i < end; i++ {
		b.Run(fmt.Sprintf("%d", dataLengths[i]), benchmarkChainedFileHasher)
	}
}

func benchmarkChainedFileHasher(b *testing.B) {
	params := strings.Split(b.Name(), "/")
	dataLength, err := strconv.ParseInt(params[1], 10, 64)
	if err != nil {
		b.Fatal(err)
	}
	_, data := generateSerialData(int(dataLength), 255, 0)
	hashFunc := func() SectionHasherTwo {
		//return newTreeHasherWrapper()
		return &wrappedHasher{
			SectionWriter: newAsyncHasher(),
		}
	}
	dataHasher := newSyncHasher()
	fh, err := NewFileSplitterTwo(dataHasher, hashFunc)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		//_ = SectionHasherTwo(fh)
		l := int64(4096)
		offset := int64(0)
		//		for j := int64(0); j < dataLength; j += 32 {
		//			remain := dataLength - offset
		//			if remain < l {
		//				l = remain
		//			}
		//			fh.Write(int(offset/32), data[offset:offset+l])
		//			offset += 32
		//		}
		for j := int64(0); j < dataLength; j += 4096 {
			remain := dataLength - offset
			if remain < l {
				l = remain
			}
			fh.Write(int(offset/32), data[offset:offset+l])
			offset += 4096
		}
		refHash := fh.Sum(nil, 0, nil)
		log.Info("res", "l", dataLength, "h", refHash)
	}
}

func TestReferenceFileHasher(t *testing.T) {
	pool = bmt.NewTreePool(sha3.NewLegacyKeccak256, 128, bmt.PoolSize)
	h := bmt.New(pool)
	var mismatch int
	for i := start; i < end; i++ {
		dataLength := dataLengths[i]
		log.Info("start", "i", i, "len", dataLength)
		fh := NewReferenceFileHasher(h, 128)
		_, data := generateSerialData(dataLength, 255, 0)
		refHash := fh.Hash(bytes.NewReader(data), len(data)).Bytes()
		eq := true
		if expected[i] != fmt.Sprintf("%x", refHash) {
			mismatch++
			eq = false
		}
		t.Logf("[%7d+%4d]\t%v\tref: %x\texpect: %s", dataLength/chunkSize, dataLength%chunkSize, eq, refHash, expected[i])
	}
	if mismatch > 0 {
		t.Fatalf("mismatches: %d/%d", mismatch, end-start)
	}
}

func TestPyramidHasherCompare(t *testing.T) {

	start = 6
	end = 7
	var mismatch int
	for i := start; i < end; i++ {
		dataLength := dataLengths[i]
		log.Info("start", "i", i, "len", dataLength)
		_, data := generateSerialData(int(dataLength), 255, 0)
		buf := bytes.NewReader(data)
		buf.Seek(0, io.SeekStart)
		putGetter := newTestHasherStore(&FakeChunkStore{}, BMTHash)

		ctx := context.Background()
		refHash, wait, err := PyramidSplit(ctx, buf, putGetter, putGetter, chunk.NewTag(0, "foo", int64(dataLength/4096+1)))
		if err != nil {
			t.Fatalf(err.Error())
		}
		err = wait(ctx)
		if err != nil {
			t.Fatalf(err.Error())
		}
		eq := true
		if expected[i] != refHash.String() {
			mismatch++
			eq = false
		}
		t.Logf("[%7d+%4d]\t%v\tref: %s\texpect: %s", dataLength/chunkSize, dataLength%chunkSize, eq, refHash, expected[i])

	}
}

func BenchmarkPyramidHasherCompareAltFileHasher(b *testing.B) {

	for i := start; i < end; i++ {
		b.Run(fmt.Sprintf("%d", dataLengths[i]), benchmarkPyramidHasherCompareAltFileHasher)
	}
}

func benchmarkPyramidHasherCompareAltFileHasher(b *testing.B) {
	params := strings.Split(b.Name(), "/")
	dataLength, err := strconv.ParseInt(params[1], 10, 64)
	if err != nil {
		b.Fatal(err)
	}
	_, data := generateSerialData(int(dataLength), 255, 0)
	buf := bytes.NewReader(data)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Seek(0, io.SeekStart)
		putGetter := newTestHasherStore(&FakeChunkStore{}, BMTHash)

		ctx := context.Background()
		_, wait, err := PyramidSplit(ctx, buf, putGetter, putGetter, chunk.NewTag(0, "foo", dataLength/4096+1))
		if err != nil {
			b.Fatalf(err.Error())
		}
		err = wait(ctx)
		if err != nil {
			b.Fatalf(err.Error())
		}
	}
}

func BenchmarkReferenceHasher(b *testing.B) {
	for i := start; i < end; i++ {
		b.Run(fmt.Sprintf("%d", dataLengths[i]), benchmarkReferenceFileHasher)
	}
}

func benchmarkReferenceFileHasher(b *testing.B) {
	params := strings.Split(b.Name(), "/")
	dataLength, err := strconv.ParseInt(params[1], 10, 64)
	if err != nil {
		b.Fatal(err)
	}
	_, data := generateSerialData(int(dataLength), 255, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := bmt.New(pool)
		fh := NewReferenceFileHasher(h, 128)
		fh.Hash(bytes.NewReader(data), len(data)).Bytes()
	}
}
