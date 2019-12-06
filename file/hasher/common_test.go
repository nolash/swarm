package hasher

import (
	"bytes"
	"context"
	"hash"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethersphere/swarm/log"
	"github.com/ethersphere/swarm/param"
	"github.com/ethersphere/swarm/testutil"
	"golang.org/x/crypto/sha3"
)

const (
	sectionSize = 32
	branches    = 128
	chunkSize   = 4096
)

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
		chunkSize * 128 * 128,  // 19
		chunkSize*128*128 + 32, // 20
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
		"ed0cc44c93b14fef2d91ab3a3674eeb6352a42ac2f0bbe524711824aae1e7bcc", // 20
	}

	start = 0
	end   = 14 //len(dataLengths)
)

var (
	dummyHashFunc = func() param.SectionWriter {
		return newDummySectionWriter(chunkSize*branches, sectionSize, sectionSize, branches)
	}

	// placeholder for cases where a hasher is not necessary
	noHashFunc = func() param.SectionWriter {
		return nil
	}

	logErrFunc = func(err error) {
		log.Error("SectionWriter pipeline error", "err", err)
	}
)

func init() {
	testutil.Init()
}

// simple param.SectionWriter hasher that keeps the data written to it
// for later inspection
// TODO: see if this can be replaced with the fake hasher from storage module
type dummySectionWriter struct {
	sectionSize int
	digestSize  int
	branches    int
	data        []byte
	digest      []byte
	size        int
	summed      bool
	writer      hash.Hash
	mu          sync.Mutex
	wg          sync.WaitGroup
}

func newDummySectionWriter(cp int, sectionSize int, digestSize int, branches int) *dummySectionWriter {
	return &dummySectionWriter{
		sectionSize: sectionSize,
		digestSize:  digestSize,
		branches:    branches,
		data:        make([]byte, cp),
		writer:      sha3.NewLegacyKeccak256(),
		digest:      make([]byte, digestSize),
	}
}

func (d *dummySectionWriter) Init(_ context.Context, _ func(error)) {
}

func (d *dummySectionWriter) Link(_ func() param.SectionWriter) {
}

// implements param.SectionWriter
func (d *dummySectionWriter) Write(index int, data []byte) {
	d.mu.Lock()
	copy(d.data[index*d.sectionSize:], data)
	d.size += len(data)
	log.Trace("dummywriter write", "index", index, "size", d.size, "threshold", d.sectionSize*d.branches)
	if d.isFull() {
		d.summed = true
		d.mu.Unlock()
		d.sum()
	} else {
		d.mu.Unlock()
	}
}

// implements param.SectionWriter
func (d *dummySectionWriter) WriteAll(data []byte) {
	d.mu.Lock()
	copy(d.data, data)
	d.size += len(data)
	d.mu.Unlock()
	log.Trace("dummywriter writeall", "size", d.size, "threshold", d.sectionSize*d.branches)
	d.summed = true
	d.sum()
}

// implements param.SectionWriter
func (d *dummySectionWriter) Sum(_ []byte, size int, _ []byte) []byte {
	log.Trace("dummy Sumcall", "size", size)
	d.mu.Lock()
	if !d.summed {
		d.size = size
		d.summed = true
		d.mu.Unlock()
		d.sum()
	} else {
		d.mu.Unlock()
	}
	return d.digest
}

func (d *dummySectionWriter) sum() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := 0; i < d.size; i += d.writer.Size() {
		sectionData := d.data[i : i+d.writer.Size()]
		log.Trace("dummy sum write", "i", i/d.writer.Size(), "data", hexutil.Encode(sectionData), "size", d.size)
		d.writer.Write(sectionData)
	}
	copy(d.digest, d.writer.Sum(nil))
	log.Trace("dummy sum result", "ref", hexutil.Encode(d.digest))
}

// implements param.SectionWriter
func (d *dummySectionWriter) Reset(_ context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data = make([]byte, len(d.data))
	d.digest = make([]byte, d.digestSize)
	d.size = 0
	d.summed = false
	d.writer.Reset()
}

// implements param.SectionWriter
func (d *dummySectionWriter) SectionSize() int {
	return d.sectionSize
}

// implements param.SectionWriter
func (d *dummySectionWriter) DigestSize() int {
	return d.sectionSize
}

// implements param.SectionWriter
func (d *dummySectionWriter) Branches() int {
	return d.branches
}

func (d *dummySectionWriter) isFull() bool {
	return d.size == d.sectionSize*d.branches
}

// TestDummySectionWriter
func TestDummySectionWriter(t *testing.T) {

	w := newDummySectionWriter(chunkSize*2, sectionSize, sectionSize, branches)
	w.Reset(context.Background())

	_, data := testutil.SerialData(sectionSize*2, 255, 0)

	w.Write(branches, data[:sectionSize])
	w.Write(branches+1, data[sectionSize:])
	if !bytes.Equal(w.data[chunkSize:chunkSize+sectionSize*2], data) {
		t.Fatalf("Write pos %d: expected %x, got %x", chunkSize, w.data[chunkSize:chunkSize+sectionSize*2], data)
	}

	correctDigestHex := "0xfbc16f6db3534b456cb257d00148127f69909000c89f8ce5bc6183493ef01da1"
	digest := w.Sum(nil, chunkSize*2, nil)
	digestHex := hexutil.Encode(digest)
	if digestHex != correctDigestHex {
		t.Fatalf("Digest: 2xsectionSize*1; expected %s, got %s", correctDigestHex, digestHex)
	}

	w = newDummySectionWriter(chunkSize*2, sectionSize*2, sectionSize*2, branches/2)
	w.Reset(context.Background())
	w.Write(branches/2, data)
	if !bytes.Equal(w.data[chunkSize:chunkSize+sectionSize*2], data) {
		t.Fatalf("Write pos %d: expected %x, got %x", chunkSize, w.data[chunkSize:chunkSize+sectionSize*2], data)
	}

	correctDigestHex += zeroHex
	digest = w.Sum(nil, chunkSize*2, nil)
	digestHex = hexutil.Encode(digest)
	if digestHex != correctDigestHex {
		t.Fatalf("Digest 1xsectionSize*2; expected %s, got %s", correctDigestHex, digestHex)
	}
}
