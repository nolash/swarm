package pot

import (
	"fmt"

	"github.com/ethersphere/swarm/log"
	"github.com/ethersphere/swarm/testutil"
)

type dumper struct {
	p   *Pot
	pos int
}

func init() {
	testutil.Init()
}

func (d *dumper) MarshalBinary() ([]byte, error) {
	var b []byte
	b = append(b, ToBytes(d.p.pin)...)
	for i := len(d.p.bins) - 1; i > -1; i-- {
		sp := d.p.bins[i]
		log.Trace("marshal", "po", sp.po, "pos", d.pos)
		if d.pos == 0 {
			b = append(b, byte(sp.po))
		} else { // attach the next po across the byte boundary
			b[len(b)-1] |= byte(sp.po) >> (8 - d.pos)
			b = append(b, byte(sp.po)<<d.pos)
		}
		b = append(b, poTruncate(ToBytes(sp.pin), sp.po, d.pos)...)
		d.pos = (d.pos + sp.po) % 8
	}
	return b, nil
}

func newDumper(p *Pot) *dumper {
	return &dumper{
		p: p,
	}
}

func poTruncate(b []byte, po int, offset int) []byte {
	byt, pos := bitByte(po)
	bsrc := b[byt:]
	if pos == 0 && offset == 0 {
		return bsrc
	}
	var bdst []byte
	shf := offset - pos
	log.Trace("bsrc", "x", fmt.Sprintf("%x", bsrc), "pos", pos, "byt", byt, "shf", shf)
	if shf <= 0 {
		bdst = make([]byte, len(bsrc))
		shf *= -1
		for i := 0; i < len(bsrc)-1; i++ {
			bdst[i] = (bsrc[i] << shf) & 0xff
			log.Trace("bdst before", "i", i, "b", fmt.Sprintf("%x", bdst))
			nx := bsrc[i+1] >> (8 - shf)
			bdst[i] |= nx & 0xff
			log.Trace("bdst after", "i", i, "b", fmt.Sprintf("%x", bdst), "nx", nx)
		}

		ls := bsrc[len(bsrc)-1]
		bdst[len(bdst)-1] = ls << shf & 0xff
		log.Trace("bdst", "b", fmt.Sprintf("%x", bdst), "ls", ls)
	} else {
		bdst = make([]byte, len(bsrc)+1)
		for i := 0; i < len(bsrc); i++ {
			bdst[i] |= (bsrc[i] >> shf) & 0xff
			log.Trace("bdst before", "i", i, "b", fmt.Sprintf("%x", bdst))
			nx := bsrc[i] << (8 - shf)
			bdst[i+1] |= nx & 0xff
			log.Trace("bdst after", "i", i, "b", fmt.Sprintf("%x", bdst), "nx", nx)
		}
	}
	return bdst
}

func bitByte(bit int) (int, int) {
	return bit / 8, bit % 8
}
