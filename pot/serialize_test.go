package pot

import (
	"bytes"
	"testing"
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
		t.Fatalf("packaddress; expected %x, got %x", c, bp)
	}
	c = []byte{0xf0, 0x20}
	bp = poTruncate(b, 5, 0)
	if !bytes.Equal(bp, c) {
		t.Fatalf("packaddress; expected %x, got %x", c, bp)
	}
	b = []byte{0xab, 0x07, 0x81}
	c = []byte{0xf0, 0x20}
	bp = poTruncate(b, 13, 0)
	if !bytes.Equal(bp, c) {
		t.Fatalf("packaddress; expected %x, got %x", c, bp)
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
