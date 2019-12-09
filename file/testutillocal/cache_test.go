package testutillocal

import (
	"bytes"
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethersphere/swarm/testutil"
)

const (
	sectionSize = 32
	chunkSize   = 4096
)

func init() {
	testutil.Init()
}

func TestCache(t *testing.T) {
	c := NewCache()
	c.Init(context.Background(), func(error) {})
	_, data := testutil.SerialData(chunkSize, 255, 0)
	c.Write(data)
	cachedData := c.Get(0)
	if !bytes.Equal(cachedData, data) {
		t.Fatalf("cache data; expected %x, got %x", data, cachedData)
	}
}

func TestCacheLink(t *testing.T) {

	hashFunc := NewBMTHasherFunc(0)

	c := NewCache()
	c.Init(context.Background(), func(error) {})
	c.SetWriter(hashFunc)
	_, data := testutil.SerialData(chunkSize, 255, 0)
	c.Seek(-1, 0)
	c.Write(data)
	ref := c.Sum(nil)
	refHex := hexutil.Encode(ref)
	correctRefHex := "0xc10090961e7682a10890c334d759a28426647141213abda93b096b892824d2ef"
	if refHex != correctRefHex {
		t.Fatalf("cache link; expected %s, got %s", correctRefHex, refHex)
	}

	c.Delete(0)
	if _, ok := c.data[0]; ok {
		t.Fatalf("delete; expected not found")
	}
}
