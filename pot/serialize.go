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
			b = append(b, poShift(ToBytes(sp.pin), sp.po, d.pos)...) //, d.pos)...)
		} else { // attach the next po across the byte boundary
			poBytes := poShift([]byte{0x00, byte(sp.po)}, d.pos, 0)
			log.Trace("marshal pobytes", "pobytes", fmt.Sprintf("%x", poBytes))
			b[len(b)-1] |= poBytes[0]
			b = append(b, poBytes[1])
			bn := poShift(ToBytes(sp.pin), sp.po, d.pos) //, d.pos)
			log.Trace("marshal shifted", "res", fmt.Sprintf("%x", bn), "src", fmt.Sprintf("%x", ToBytes(sp.pin)), "b", fmt.Sprintf("%x", b))
			b[len(b)-1] |= bn[0]
			if len(bn) > 1 {
				b = append(b, bn[1:]...)
			}
		}
		d.pos = (d.pos + sp.po) % 8
	}
	return b, nil
}

func newDumper(p *Pot) *dumper {
	return &dumper{
		p: p,
	}
}

// returns the byte slice left-shifted to the order of po and right-shifted to the order of offset
// offset should be a value within a single byte offset. If offset>7, result is undefined
func poShift(b []byte, po int, offset int) []byte {
	byt, pos := bitByte(po)
	bsrc := b[byt:]
	if pos == 0 && offset == 0 {
		return bsrc
	}
	var bdst []byte
	shf := (offset + pos) % 8
	log.Trace("bsrc", "x", fmt.Sprintf("%x", bsrc), "pos", pos, "byt", byt, "shf", shf, "offset", offset)
	//if shf < 0 {
	bdst = make([]byte, len(bsrc))
	//shf *= -1
	for i := 0; i < len(bsrc)-1; i++ {
		bdst[i] = (bsrc[i] << shf) & 0xff
		log.Trace("bdst shf- before", "i", i, "b", fmt.Sprintf("%x", bdst))
		nx := bsrc[i+1] >> (8 - shf)
		bdst[i] |= nx & 0xff
		log.Trace("bdst shf- after", "i", i, "b", fmt.Sprintf("%x", bdst), "nx", nx)
	}

	ls := bsrc[len(bsrc)-1]
	bdst[len(bdst)-1] = ls << shf & 0xff
	log.Trace("bdst", "b", fmt.Sprintf("%x", bdst), "ls", ls)
	return bdst
}

func bitByte(bit int) (int, int) {
	return bit / 8, bit % 8
}
