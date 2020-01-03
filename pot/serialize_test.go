package pot

import (
	"bytes"
	"fmt"
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

func TestSerializeTailLength(t *testing.T) {
	l := tailLength(3, 15)
	if l != 24-15 {
		t.Fatalf("taillength; expected %d, got %d", 24-15, l)
	}
}

//func TestSerializeShiftAcrossBytes(t *testing.T) {
//	a := byte(0x2a) // 0010 1010
//	result := shiftAcrossBytes(a, 3)
//	correct := []byte{0x05, 0x40} // 0000 0101 0100 0000
//	if !bytes.Equal(result, correct) {
//		t.Fatalf("shiftacross > 0; expected %x, got %x", correct, result)
//	}
//
//	result = shiftAcrossBytes(a, -3)
//	correct = []byte{0x01, 0x50} // 0000 0001 0101 0000
//	if !bytes.Equal(result, correct) {
//		t.Fatalf("shiftacross < 0; expected %x, got %x", correct, result)
//	}
//}

func TestSerializePackAddress(t *testing.T) {
	b := []byte{0x07, 0x81}
	c := []byte{0x81}
	bp := poShift(b, 8, 0)
	if !bytes.Equal(bp, c) {
		t.Fatalf("packaddress 8/0; expected %x, got %x", c, bp)
	}
	c = []byte{0xf0, 0x20}
	bp = poShift(b, 5, 0)
	if !bytes.Equal(bp, c) {
		t.Fatalf("packaddress 5/0; expected %x, got %x", c, bp)
	}
	b = []byte{0xab, 0x07, 0x81}
	c = []byte{0xf0, 0x20}
	bp = poShift(b, 13, 0)
	if !bytes.Equal(bp, c) {
		t.Fatalf("packaddress 13/0; expected %x, got %x", c, bp)
	}

	b = []byte{0xab, 0x07, 0x81}
	c = []byte{0x1e, 0x04}
	bp = poShift(b, 13, 5)
	if !bytes.Equal(bp, c) {
		t.Fatalf("packaddress 13/5; expected %x, got %x", c, bp)
	}
}

func TestSerializeSingle(t *testing.T) {
	pof := DefaultPof(255)
	a := make([]byte, 32)
	b := make([]byte, 32)
	b[10] = 0x80
	p := NewPot(a, 0)
	p, pob, _ := Add(p, b, pof)
	d := newDumper(p)
	s, err := d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	correct := append(a, byte(pob))
	correct = append(correct, byte(1))
	correct = append(correct, b[10:]...)
	if !bytes.Equal(s, correct) {
		t.Fatalf("prefix mismatch 1; expected %x, got %x", correct, s)
	}

	b[10] = 0x04
	p = NewPot(a, 0)
	p, pob, _ = Add(p, b, pof)

	correct = make([]byte, 32+23+1)
	correct[32] = byte(pob)
	correct[33] = 0x01
	correct[34] = 0x80
	d = newDumper(p)
	s, err = d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s, correct) {
		t.Fatalf("prefix mismatch 2; expected %x, got %x", correct, s)
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
	p, pob, _ := Add(p, b, pof)
	p, poc, _ := Add(p, c, pof)
	p, pod, _ := Add(p, d, pof)
	dm := newDumper(p)
	s, err := dm.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	crsr := 32
	correct := make([]byte, 32+31+30+29+3+3) // 32 length a, 31 length b, 30 length c, 29 length d, 3 po index bytes, 3 size bytes
	correct[crsr] = byte(pob)                // the po follow right after the root pin
	crsr += 1
	correct[crsr] = byte(1)
	crsr += 1
	correct[crsr] = 0x80
	crsr += 29
	correct[crsr] = byte(poc)
	crsr += 1
	correct[crsr] = byte(1)
	crsr += 1
	correct[crsr] = 0x80
	crsr += 30
	correct[crsr] = byte(pod)
	crsr += 1
	correct[crsr] = byte(1)
	crsr += 1
	correct[crsr] = 0x80
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
	a := make([]byte, 3)
	b := make([]byte, 3)
	c := make([]byte, 3)
	b[2] = 0x02
	c[1] = 0x04
	p := NewPot(a, 0)
	p, pob, _ := Add(p, b, pof)
	p, poc, _ := Add(p, c, pof)
	d := newDumper(p)
	s, err := d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	log.Trace("po", "b", pob, "c", poc)

	correct := make([]byte, len(a)+2+2+1+2-1) // length of root pin data, two po byte prefixes, two bin length prefixes, 1 byte data for b, 2 bytes data for c, -1 bytes data for total shifts rounded down (6+5=11, 11/8= 1)
	crsr := 3                                 // after root pin data
	po := (pob % 8)                           // shift position of first fork
	correct[crsr] = byte(pob)                 // the po follow right after the root pin on byte boundary
	crsr += 1
	correct[crsr] = byte(1) // the po follow right after the root pin on byte boundary
	crsr += 1
	correct[crsr] = b[2] << po // shifts the bit from the first fork in place

	poBytes := poShift([]byte{0x00, byte(poc), byte(1)}, po, 0) // the next po prefix spans across byte boundary according to po of b
	correct[crsr] |= poBytes[0]                                 // the data of b is 2 bits, so we mask from the same byte
	crsr += 1
	correct[crsr] = poBytes[1] // 0000 1101 -> 1000 0011 0100 0000, shifted 6, first bit is first fork bit
	crsr += 1
	correct[crsr] = poBytes[2] // 0000 1101 -> 1000 0011 0100 0000, shifted 6, first bit is first fork bit

	po = (po + poc) % 8         // the shift is now the sum of b po and c po
	correct[crsr] |= c[1] << po // shift the c data in place

	if !bytes.Equal(s, correct) {
		t.Fatalf("serialize two - zeros after fork; expected %x, got %x", correct, s)
	}
}

func TestSerializeMore(t *testing.T) {
	pof := DefaultPof(255)
	a := make([]byte, 3)
	b := make([]byte, 3)
	c := make([]byte, 3)
	d := make([]byte, 3)

	b[2] = 0x02
	c[1] = 0x04
	d[1] = 0x80
	p := NewPot(a, 0)
	p, pob, _ := Add(p, b, pof)
	p, poc, _ := Add(p, c, pof)
	p, pod, _ := Add(p, d, pof)
	dm := newDumper(p)
	s, err := dm.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	log.Trace("correct calc")

	correct := make([]byte, len(a)+2+2+2+1+1+1+1)
	crsr := 3                 // after root pin data
	po := (pob % 8)           // shift position of first fork
	correct[crsr] = byte(pob) // the po follow right after the root pin on byte boundary
	crsr += 1
	correct[crsr] = byte(1) // the po follow right after the root pin on byte boundary
	crsr += 1
	correct[crsr] = b[2] << po // shifts the bit from the first fork in place

	poBytes := poShift([]byte{0x00, byte(poc), byte(1)}, po, 0) // the next po prefix spans across byte boundary according to po of b
	correct[crsr] |= poBytes[0]                                 // the data of b is 2 bits, so we mask from the same byte
	crsr += 1
	correct[crsr] = poBytes[1] // 0000 1101 -> 1000 0011 0100 0000, shifted 6, first bit is first fork bit
	crsr += 1
	correct[crsr] = poBytes[2]

	po = (po + poc) % 8         // the shift is now the sum of b po and c po
	correct[crsr] |= c[1] << po // shift the c data in place

	poBytes = poShift([]byte{0x00, byte(pod), byte(1)}, po, 0) // the next po prefix spans across byte boundary according to po of b
	crsr += 1
	correct[crsr] |= poBytes[0] // the data of b is 2 bits, so we mask from the same byte
	crsr += 1
	correct[crsr] = poBytes[1] // 0000 1101 -> 1000 0011 0100 0000, shifted 6, first bit is first fork bit
	crsr += 1
	correct[crsr] = poBytes[2]

	po = (po + pod) % 8 // the shift is now the sum of b po and c po
	poBytes = poShift([]byte{0x00, d[1]}, po, 0)
	correct[crsr] |= poBytes[0]

	if !bytes.Equal(s, correct) {
		t.Fatalf("serialize two - zeros after fork; expected %x, got %x", correct, s)
	}
}

func TestSerializeNested(t *testing.T) {
	pof := DefaultPof(255)
	a := make([]byte, 3)
	b := make([]byte, 3)
	c := make([]byte, 3)
	d := make([]byte, 3)

	b[2] = 0x04
	c[2] = 0x06
	d[1] = 0x80

	p := NewPot(a, 0)
	p, pob, _ := Add(p, b, pof)
	p, _, _ = Add(p, c, pof)
	p, pod, _ := Add(p, d, pof)
	dm := newDumper(p)
	s, err := dm.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	correct := make([]byte, len(a)+2+2+2+1+1+1)
	crsr := 3
	po := (pob % 8)
	correct[crsr] = byte(pob)
	crsr += 1
	correct[crsr] = byte(2)
	crsr += 1
	correct[crsr] = b[2] << po

	poc, _ := pof(b, c, 0)
	poBytes := poShift([]byte{0x00, byte(poc), byte(1)}, po, 0)
	correct[crsr] |= poBytes[0]
	crsr += 1
	correct[crsr] = poBytes[1]
	crsr += 1
	correct[crsr] = poBytes[2]

	po = (po + poc) % 8 // the shift is now the sum of b po and c po
	log.Trace("second po", "Po", po, "correct", fmt.Sprintf("%x", correct))
	correct[crsr] |= c[2] << po
	log.Trace("second po", "Po", po, "correct", fmt.Sprintf("%x", correct))

	poBytes = poShift([]byte{0x00, byte(pod), byte(1)}, po, 0)
	correct[crsr] |= poBytes[0]
	crsr += 1
	correct[crsr] = poBytes[1]
	crsr += 1
	correct[crsr] = poBytes[2]

	po = (po + pod) % 8 // the shift is now the sum of b po and c po
	log.Trace("last po", "po", po)
	poBytes = poShift([]byte{0x00, d[1]}, po, 0)
	correct[crsr] |= poBytes[0]

	if !bytes.Equal(s, correct) {
		t.Fatalf("serialize nested - zeros after fork; expected %x, got %x", correct, s)
	}

}
