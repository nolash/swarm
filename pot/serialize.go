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
	return d.marshalBinary(d.p, b)
}

func (d *dumper) marshalBinary(p *Pot, b []byte) ([]byte, error) {
	for i := len(p.bins) - 1; i > -1; i-- {
		sp := p.bins[i]

		log.Trace("marshal", "po", sp.po, "pos", d.pos, "size", sp.size, "pot", fmt.Sprintf("%p", sp), "b", fmt.Sprintf("%p", b), "lenb", len(b))
		if d.pos == 0 {
			b = append(b, byte(sp.po))
			b = append(b, byte(sp.size))                             // TODO make this a varint
			b = append(b, poShift(ToBytes(sp.pin), sp.po, d.pos)...) //, d.pos)...)
		} else { // attach the next po across the byte boundary
			poBytes := poShift([]byte{0x00, byte(sp.po), byte(sp.size)}, d.pos, 0)
			log.Trace("marshal pobytes", "pobytes", fmt.Sprintf("%x", poBytes))
			b[len(b)-1] |= poBytes[0]
			b = append(b, poBytes[1:]...)
			bn := poShift(ToBytes(sp.pin), sp.po, d.pos) //, d.pos)
			log.Trace("marshal shifted", "res", fmt.Sprintf("%x", bn), "src", fmt.Sprintf("%x", ToBytes(sp.pin)), "b", fmt.Sprintf("%x", b), "lenb", len(b))
			b[len(b)-1] |= bn[0]
			if len(bn) > 1 {
				b = append(b, bn[1:]...)
			}
		}
		d.pos = (d.pos + sp.po) % 8
		for j := 1; j < sp.size; j++ {
			var err error
			log.Trace("nested call", "pot", fmt.Sprintf("%p", sp))
			b, err = d.marshalBinary(sp, b)
			if err != nil {
				return nil, err
			}
		}
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
	bdst = make([]byte, len(bsrc))
	if shf <= pos {
		for i := 0; i < len(bsrc)-1; i++ {
			log.Trace("bdst shf- before", "i", i, "b", fmt.Sprintf("%x", bdst))
			bdst[i] = (bsrc[i] << shf) & 0xff
			nx := bsrc[i+1] >> (8 - shf)
			bdst[i] |= nx & 0xff
			log.Trace("bdst shf- after", "i", i, "b", fmt.Sprintf("%x", bdst), "nx", nx)
		}
	} else {
		for i := 0; i < len(bsrc)-1; i++ {
			log.Trace("bdst shf+ before", "i", i, "b", fmt.Sprintf("%x", bdst))
			bdst[i] = (bsrc[i] >> (8 - shf)) & 0xff
			nx := bsrc[i+1] << shf
			bdst[i] |= nx & 0xff
			log.Trace("bdst shf+ after", "i", i, "b", fmt.Sprintf("%x", bdst), "nx", nx)
		}
	}

	ls := bsrc[len(bsrc)-1]
	bdst[len(bdst)-1] = ls << shf & 0xff
	log.Trace("bdst", "b", fmt.Sprintf("%x", bdst), "ls", ls)
	return bdst
}

func bitByte(bit int) (int, int) {
	return bit / 8, bit % 8
}
