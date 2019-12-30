package pot

import (
	"fmt"

	"github.com/ethersphere/swarm/log"
	"github.com/ethersphere/swarm/testutil"
)

type dumper struct {
	p *Pot
}

func init() {
	testutil.Init()
}

func (d *dumper) MarshalBinary() ([]byte, error) {
	var b []byte
	b = append(b, ToBytes(d.p.pin)...)
	for i := len(d.p.bins) - 1; i > -1; i-- {
		sp := d.p.bins[i]
		b = append(b, byte(sp.po))
		b = append(b, poTruncate(ToBytes(sp.pin), sp.po)...)
	}
	return b, nil
}

func newDumper(p *Pot) *dumper {
	return &dumper{
		p: p,
	}
}

func poTruncate(b []byte, po int) []byte {
	byt, pos := bitByte(po)
	bsrc := b[byt:]
	if pos == 0 {
		return bsrc
	}
	log.Trace("bsrc", "x", fmt.Sprintf("%x", bsrc), "pos", pos, "byt", byt)
	bdst := make([]byte, len(bsrc))
	for i := 0; i < len(bsrc)-1; i++ {
		bdst[i] = (bsrc[i] << pos) & 0xff
		log.Trace("bdst", "i", i, "b", fmt.Sprintf("%x", bdst))
		nx := bsrc[i+1] >> (8 - pos)
		bdst[i] |= nx & 0xff
		log.Trace("bdst", "i", i, "b", fmt.Sprintf("%x", bdst), "nx", nx)
	}
	ls := bsrc[len(bsrc)-1]
	bdst[len(bdst)-1] = ls << pos & 0xff
	log.Trace("bdst", "b", fmt.Sprintf("%x", bdst), "ls", ls)
	return bdst
}

func bitByte(bit int) (int, int) {
	return bit / 8, bit % 8
}
