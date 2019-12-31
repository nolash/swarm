package pot

import (
	"bytes"
	"testing"

	"github.com/ethersphere/swarm/log"
)

func TestSerializeFindBitByte(t *testing.T) {
	ins := []int{0, 1, 7, 8, 62, 64}
	outs := []int{0, 0, 0, 1, 7, 8}
	poss := []int{0, 1, 7, 0, 6, 0}
	for i, in := range ins {
		byt, pos := bitByte(in)
		if byt != outs[i] {
			t.Fatalf("bitbyte byte for %d; expected %d, got %d", in, outs[i], byt)
		}
		if pos != poss[i] {
			t.Fatalf("bitbyte pos for %d; expected %d, got %d", in, poss[i], pos)
		}
	}
}

func TestSerializePackAddress(t *testing.T) {
	b := []byte{0x07, 0x81}
	c := []byte{0x81}
	bp := poTruncate(b, 8, 0)
	if !bytes.Equal(bp, c) {
		t.Fatalf("packaddress 8/0; expected %x, got %x", c, bp)
	}
	c = []byte{0xf0, 0x20}
	bp = poTruncate(b, 5, 0)
	if !bytes.Equal(bp, c) {
		t.Fatalf("packaddress 5/0; expected %x, got %x", c, bp)
	}
	b = []byte{0xab, 0x07, 0x81}
	c = []byte{0xf0, 0x20}
	bp = poTruncate(b, 13, 0)
	if !bytes.Equal(bp, c) {
		t.Fatalf("packaddress 13/0; expected %x, got %x", c, bp)
	}

	b = []byte{0xab, 0x07, 0x81}
	c = []byte{0x07, 0x81}
	bp = poTruncate(b, 13, 5)
	if !bytes.Equal(bp, c) {
		t.Fatalf("packaddress 13/5; expected %x, got %x", c, bp)
	}

	b = []byte{0xab, 0x07, 0x81} // 0000 0111 1000 0001
	c = []byte{0x00, 0x3c, 0x08} // 0000 0000 0011 1100 0000 1000
	bp = poTruncate(b, 8, 5)
	if !bytes.Equal(bp, c) {
		t.Fatalf("packaddress 8/5; expected %x, got %x", c, bp)
	}
}

func TestSerializeSingle(t *testing.T) {
	pof := DefaultPof(255)
	a := make([]byte, 32)
	b := make([]byte, 32)
	b[10] = 0x80
	p := NewPot(a, 0)
	p, _, _ = Add(p, b, pof)
	d := newDumper(p)
	s, err := d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	correct := append(a, byte(80))
	correct = append(correct, b[10:]...)
	if !bytes.Equal(s, correct) {
		t.Fatalf("prefix match; expected %x, got %x", correct, s)
	}

	b[10] = 0x04
	p = NewPot(a, 0)
	p, _, _ = Add(p, b, pof)

	correct = make([]byte, 32+23)
	correct[32] = byte(85)
	correct[33] = 0x80
	d = newDumper(p)
	s, err = d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s, correct) {
		t.Fatalf("prefix match; expected %x, got %x", correct, s)
	}
}

func TestSerializeBoundary(t *testing.T) {
	pof := DefaultPof(255)
	a := make([]byte, 32)
	b := make([]byte, 32)
	c := make([]byte, 32)
	d := make([]byte, 32)

	b[3] = 0x80
	c[2] = 0x80
	d[1] = 0x80
	p := NewPot(a, 0)
	p, _, _ = Add(p, b, pof)
	p, _, _ = Add(p, c, pof)
	p, _, _ = Add(p, d, pof)
	dm := newDumper(p)
	s, err := dm.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	correct := make([]byte, 32+31+30+29+3)
	correct[32] = byte(8 * 3) // the po follow right after the root pin
	correct[32+1] = 0x80
	correct[32+1+29] = byte(8 * 2)
	correct[32+1+29+1] = 0x80
	correct[32+1+29+1+30] = byte(8)
	correct[32+1+29+1+30+1] = 0x80
	if !bytes.Equal(s, correct) {
		t.Fatalf("serialize boundary - zeros after fork; expected %x, got %x", correct, s)
	}
}

func TestSerializeDumperPos(t *testing.T) {
	pof := DefaultPof(255)
	a := make([]byte, 32)
	b := make([]byte, 32)
	c := make([]byte, 32)
	d := make([]byte, 32)
	b[3] = 0x02
	c[2] = 0x04
	d[1] = 0x08
	p := NewPot(a, 0)
	p, _, _ = Add(p, b, pof)
	dm := newDumper(p)
	dm.MarshalBinary()
	po := (3 * 8) + 6
	pos := po % 8
	if dm.pos != pos {
		t.Fatalf("dumper pos after one child at 0x00000002 (%d); expected pos %d, got %d", po, pos, dm.pos)
	}
	log.Trace("pos", "p", pos)

	p, _, _ = Add(p, c, pof)
	dm = newDumper(p)
	dm.MarshalBinary()
	po = (2 * 8) + 5
	pos = (pos + po) % 8
	log.Trace("pos", "p", pos)
	if dm.pos != pos {
		t.Fatalf("dumper pos after second child at 0x000004 (%d); expected pos %d, got %d", po, pos, dm.pos)
	}

	p, _, _ = Add(p, d, pof)
	dm = newDumper(p)
	dm.MarshalBinary()
	po = (1 * 8) + 4
	pos = (pos + po) % 8
	log.Trace("pos", "p", pos)
	if dm.pos != pos {
		t.Fatalf("dumper pos after third child at 0x0008 (%d); expected pos %d, got %d", po, pos, dm.pos)
	}

}

func TestSerializeTwo(t *testing.T) {
	pof := DefaultPof(255)
	a := make([]byte, 32)
	b := make([]byte, 32)
	c := make([]byte, 32)
	b[3] = 0x02
	c[2] = 0x04
	p := NewPot(a, 0)
	p, _, _ = Add(p, b, pof)
	p, _, _ = Add(p, c, pof)
	d := newDumper(p)
	s, err := d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	po := (3 * 8) + 6
	correct := make([]byte, 32+(32-2)+(32-3)+2-1)
	correct[32] = byte(po)        // the po follow right after the root pin
	correct[33] = 0x80            // this is the bit from the first fork
	correct[33+(32-1-3)] = 0x05   // is now shifted 6, po for second member is 21, 21 0x0015 shifted 6 is 0x0540 (first byte 03 because the data in last byte of first member is 0x00)
	correct[33+(32-1-3)+1] = 0x60 // the second byte of the po is packed with the bit in the second fork; 0100 0000 -> 0110 0000
	if !bytes.Equal(s, correct) {
		t.Fatalf("serialize two - zeros after fork; expected %x, got %x", correct, s)
	}
}

func TestSerializeMore(t *testing.T) {
	pof := DefaultPof(255)
	a := make([]byte, 32)
	b := make([]byte, 32)
	c := make([]byte, 32)
	d := make([]byte, 32)

	b[3] = 0x02
	c[2] = 0x04
	d[1] = 0x08
	p := NewPot(a, 0)
	p, _, _ = Add(p, b, pof)
	p, _, _ = Add(p, c, pof)
	p, _, _ = Add(p, d, pof)
	dm := newDumper(p)
	s, err := dm.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	po := (3 * 8) + 6
	correct := make([]byte, 32+(32-2)+(32-3)+(32-1)+3-1)
	correct[32] = byte(po)                  // the po follow right after the root pin
	correct[33] = 0x80                      // this is the bit from the first fork
	correct[33+(32-1-3)] = 0x05             // is now shifted 6, po for second member is 21, 21 0x0015 shifted 6 is 0x0540 (first byte 03 because the data in last byte of first member is 0x00)
	correct[33+(32-1-3)+1] = 0x60           // the second byte of the po is packed with the bit in the second fork; 0100 0000 -> 0110 0000
	correct[33+(32-1-3)+(32-1-2)+2] = 0x60  // is now shifted 6+5 = 11, po for third member is 12, 12 0x000c, shifted 11 is 0x6000
	correct[33+(32-1-3)+(32-1-2)+2] |= 0x04 // the boundary is on the second bit of the second nibble; 0x0c = 0000 1100;  [0]000 0110 0[100], which should result in 0x64
	if !bytes.Equal(s, correct) {
		t.Fatalf("serialize more - zeros after fork; expected %x, got %x", correct, s)
	}
}
