package network

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethersphere/swarm/p2p/protocols"
)

// TestCapabilitySetUnset tests that setting and unsetting bits yield expected results
func TestCapabilitySetUnset(t *testing.T) {
	firstSet := []bool{
		true, false, false, false, false, false, true, true, false,
	} // 1000 0011 0
	firstResult := firstSet
	secondSet := []bool{
		false, true, false, true, false, false, true, false, true,
	} // 0101 0010 1
	secondResult := []bool{
		true, true, false, true, false, false, true, true, true,
	} // 1101 0011 1
	thirdUnset := []bool{
		true, false, true, true, false, false, true, false, true,
	} // 1011 0010 1
	thirdResult := []bool{
		false, true, false, false, false, false, false, true, false,
	} // 0100 0001 0

	c := NewCapability(42, 9)
	for i, b := range firstSet {
		if b {
			c.Set(i)
		}
	}
	if !isSameBools(c.Cap, firstResult) {
		t.Fatalf("first set result mismatch, expected %v, got %v", firstResult, c.Cap)
	}

	for i, b := range secondSet {
		if b {
			c.Set(i)
		}
	}
	if !isSameBools(c.Cap, secondResult) {
		t.Fatalf("second set result mismatch, expected %v, got %v", secondResult, c.Cap)
	}

	for i, b := range thirdUnset {
		if b {
			c.Unset(i)
		}
	}
	if !isSameBools(c.Cap, thirdResult) {
		t.Fatalf("second set result mismatch, expected %v, got %v", thirdResult, c.Cap)
	}
}

// TestCapabilitiesControl tests that the methods for manipulating the capabilities bitvectors set values correctly and return errors when they should
func TestCapabilitiesControl(t *testing.T) {

	// Initialize capability
	caps := NewCapabilities()

	// Register module. Should succeed
	c1 := NewCapability(1, 16)
	err := caps.add(c1)
	if err != nil {
		t.Fatalf("RegisterCapabilityModule fail: %v", err)
	}

	// Fail if capability id already exists
	c2 := NewCapability(1, 1)
	err = caps.add(c2)
	if err == nil {
		t.Fatalf("Expected RegisterCapabilityModule call with existing id to fail")
	}

	// More than one capabilities flag vector should be possible
	c3 := NewCapability(2, 1)
	err = caps.add(c3)
	if err != nil {
		t.Fatalf("RegisterCapabilityModule (second) fail: %v", err)
	}
}

// TestCapabilitiesQuery tests methods for quering capability states
func TestCapabilitiesQuery(t *testing.T) {

	// Initialize capability
	caps := NewCapabilities()

	// Register module. Should succeed
	c1 := NewCapability(1, 3)
	c1.Set(1)
	err := caps.add(c1)
	if err != nil {
		t.Fatalf("RegisterCapabilityModule fail: %v", err)
	}

	c2 := NewCapability(42, 9)
	c2.Set(2)
	c2.Set(8)
	err = caps.add(c2)
	if err != nil {
		t.Fatalf("RegisterCapabilityModule fail: %v", err)
	}

	capsCompare := NewCapabilities()
	capCompare := NewCapability(42, 10)
	capCompare.Set(2)
	capCompare.Set(8)
	capsCompare.add(capCompare)
	if caps.Match(capsCompare) {
		t.Fatalf("Expected cCompare with mismatch length to fail; %s != %s", capsCompare, caps)
	}

	capsCompare = NewCapabilities()
	capCompare = NewCapability(42, 9)
	capCompare.Set(2)
	capsCompare.add(capCompare)
	if !caps.Match(capsCompare) {
		t.Fatalf("Expected %s to match %s", capsCompare, caps)
	}

	capCompare = NewCapability(1, 3)
	capsCompare.add(capCompare)
	if !caps.Match(capsCompare) {
		t.Fatalf("Expected %s to match %s", capsCompare, caps)
	}

	capCompare.Set(1)
	if !caps.Match(capsCompare) {
		t.Fatalf("Expected %s to match %s", capsCompare, caps)
	}

	capCompare.Set(2)
	if caps.Match(capsCompare) {
		t.Fatalf("Expected %s not to match %s", capsCompare, caps)
	}

}

// TestCapabilitiesString checks that the string representation of the capabilities is correct
func TestCapabilitiesString(t *testing.T) {
	sets1 := []bool{
		false, false, true,
	}
	c1 := NewCapability(42, len(sets1))
	for i, b := range sets1 {
		if b {
			c1.Set(i)
		}
	}
	sets2 := []bool{
		true, false, false, false, true, false, true, false, true,
	}
	c2 := NewCapability(666, len(sets2))
	for i, b := range sets2 {
		if b {
			c2.Set(i)
		}
	}

	caps := NewCapabilities()
	caps.add(c1)
	caps.add(c2)

	correctString := "42:001,666:100010101"
	if correctString != caps.String() {
		t.Fatalf("Capabilities string mismatch; expected %s, got %s", correctString, caps)
	}
}

// TestCapabilitiesRLP ensures that a round of serialization and deserialization of Capabilities object
// results in the correct data
func TestCapabilitiesRLP(t *testing.T) {
	c := NewCapabilities()
	cap1 := &Capability{
		Id:  42,
		Cap: []bool{true, false, true},
	}
	c.add(cap1)
	cap2 := &Capability{
		Id:  666,
		Cap: []bool{true, false, true, false, true, true, false, false, true},
	}
	c.add(cap2)
	buf := bytes.NewBuffer(nil)
	err := rlp.Encode(buf, &c)
	if err != nil {
		t.Fatal(err)
	}

	cRestored := NewCapabilities()
	err = rlp.Decode(buf, &cRestored)
	if err != nil {
		t.Fatal(err)
	}

	cap1Restored := cRestored.get(cap1.Id)
	if cap1Restored.Id != cap1.Id {
		t.Fatalf("cap 1 id not correct, expected %d, got %d", cap1.Id, cap1Restored.Id)
	}
	if !cap1.IsSameAs(cap1Restored) {
		t.Fatalf("cap 1 caps not correct, expected %v, got %v", cap1.Cap, cap1Restored.Cap)
	}

	cap2Restored := cRestored.get(cap2.Id)
	if cap2Restored.Id != cap2.Id {
		t.Fatalf("cap 1 id not correct, expected %d, got %d", cap2.Id, cap2Restored.Id)
	}
	if !cap2.IsSameAs(cap2Restored) {
		t.Fatalf("cap 1 caps not correct, expected %v, got %v", cap2.Cap, cap2Restored.Cap)
	}
}

func TestAdaptiveKademlia(t *testing.T) {
	capPeer := NewCapability(42, 13)
	capPeer.Set(1)
	capPeer.Set(8)
	capsPeer := NewCapabilities()
	capsPeer.add(capPeer)

	capCompare := NewCapability(42, 13)
	capCompare.Set(1)
	capsCompare := NewCapabilities()
	capsCompare.add(capCompare)

	selfAddr := RandomAddr()
	kadParams := NewKadParams()
	k := NewKademlia(selfAddr.Address(), kadParams)

	// Add the peer ot the kademlia and switch it on
	ap, err := newAdaptivePeer(capsPeer, k)
	if err != nil {
		t.Fatal(err)
	}
	err = k.Register(ap.BzzAddr)
	if err != nil {
		t.Fatal(err)
	}
	k.On(ap) // TODO why is this a method on the kademlia for a peer that already has a kademlia pointer

	found := false
	k.EachConn(selfAddr.Address(), 255, func(p *Peer, po int) bool {
		found = true
		return true
	})
	if !found {
		t.Fatalf("Expected conn to returned: cap %s should be in set cap %s", capPeer, capCompare)
	}

	found = false
	k.EachConnFiltered(selfAddr.Address(), capsCompare, 255, func(p *Peer, po int) bool {
		found = true
		return true
	})
	if !found {
		t.Fatalf("Expected conn to be returned: cap %s should be in set cap %s", capPeer, capCompare)
	}

	capCompare.Set(2)
	k.EachConnFiltered(selfAddr.Address(), capsCompare, 255, func(p *Peer, po int) bool {
		t.Fatalf("Expected no returned conn: cap %s should not be in set cap %s", capPeer, capCompare)
		return true
	})
}

func newAdaptivePeer(caps *Capabilities, k *Kademlia) (*Peer, error) {
	// create the peer that fits the kademlia record
	// it's quite a bit of work
	peerPrivKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}
	peerEnodeId := enode.PubkeyToIDV4(&peerPrivKey.PublicKey)
	peerP2p := p2p.NewPeer(peerEnodeId, "foo", []p2p.Cap{})
	peerProto := protocols.NewPeer(peerP2p, nil, nil)
	peerBzz := NewBzzPeer(peerProto)
	peerBzz.WithCapabilities(caps)
	return NewPeer(peerBzz, k), nil
}
