package network

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/rlp"
)

// CapabilityId defines a unique type of capability
// @acud concrete enough for ya?
type CapabilityId uint64

// Capability contains a bit vector of flags that define what capability a node has in a specific module
// The module is defined by the Id.
type Capability struct {
	Id  CapabilityId
	Cap []bool
}

// NewCapability initializes a new Capability with the given id and specified number of bits in the vector
func NewCapability(id CapabilityId, bitCount int) *Capability {
	return &Capability{
		Id:  id,
		Cap: make([]bool, bitCount),
	}
}

// Set switches the bit at the specified index on
func (c *Capability) Set(idx int) error {
	l := len(c.Cap)
	if idx > l-1 {
		return fmt.Errorf("index %d out of bounds (len=%d)", idx, l)
	}
	c.Cap[idx] = true
	return nil
}

// Unset switches the bit at the specified index off
func (c *Capability) Unset(idx int) error {
	l := len(c.Cap)
	if idx > l-1 {
		return fmt.Errorf("index %d out of bounds (len=%d)", idx, l)
	}
	c.Cap[idx] = false
	return nil
}

// String implements Stringer interface
func (c Capability) String() (s string) {
	s = fmt.Sprintf("%d:", c.Id)
	for _, b := range c.Cap {
		if b {
			s += "1"
		} else {
			s += "0"
		}
	}
	return s
}

// IsSameAs returns true if the given Capability object has the identical bit settings as the receiver
func (c *Capability) IsSameAs(cp *Capability) bool {
	if cp == nil {
		return false
	}
	return isSameBools(c.Cap, cp.Cap)
}

func isSameBools(left []bool, right []bool) bool {
	if len(left) != len(right) {
		return false
	}
	for i, b := range left {
		if b != right[i] {
			return false
		}
	}
	return true
}

// Capabilities is the collection of capabilities for a Swarm node
// It is used both to store the capabilities in the node, and
// to communicate the node capabilities to its peers
type Capabilities struct {
	idx  map[CapabilityId]int // maps the CapabilityIds to their position in the Caps vector
	Caps []*Capability
	mu   sync.Mutex
}

// NewCapabilities initializes a new Capabilities object
func NewCapabilities() *Capabilities {
	return &Capabilities{
		idx: make(map[CapabilityId]int),
	}
}

// Match returns true if all bits set in the argument is also set in the receiver
func (c *Capability) Match(capCompare *Capability) bool {
	if len(c.Cap) != len(capCompare.Cap) {
		return false
	}
	// on the first occurence of false where query has true we can fail
	for i, flag := range capCompare.Cap {
		if flag && !c.Cap[i] {
			return false
		}
	}
	return true
}

func (c *Capabilities) Match(capsCompare *Capabilities) bool {
	for _, capCompare := range capsCompare.Caps {

		// if queryied id doesn't exist in object we can nay right away
		cap := c.get(capCompare.Id)
		if cap == nil {
			return false
		}
		if !cap.Match(capCompare) {
			return false
		}
	}
	return true
}

// adds a capability to the Capabilities collection
func (c *Capabilities) add(cp *Capability) error {
	if _, ok := c.idx[cp.Id]; ok {
		return fmt.Errorf("Capability id %d already registered", cp.Id)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Caps = append(c.Caps, cp)
	c.idx[cp.Id] = len(c.Caps) - 1
	return nil
}

// gets the capability with the specified module id
// returns nil if the id doesn't exist
func (c Capabilities) get(id CapabilityId) *Capability {
	idx, ok := c.idx[id]
	if !ok {
		return nil
	}
	return c.Caps[idx]
}

// String Implements Stringer interface
func (c Capabilities) String() (s string) {
	for _, cp := range c.Caps {
		if s != "" {
			s += ","
		}
		s += cp.String()
	}
	return s
}

// DecodeRLP implements rlp.RLPDecoder
// this custom deserializer builds the module id to array index map
// state of receiver is undefined on error
func (c *Capabilities) DecodeRLP(s *rlp.Stream) error {

	// make sure we have a pristine receiver
	c.idx = make(map[CapabilityId]int)
	c.Caps = []*Capability{}

	// discard the Capabilities struct list item
	_, err := s.List()
	if err != nil {
		return err
	}

	// discard the Capabilities Caps array list item
	_, err = s.List()
	if err != nil {
		return err
	}

	// All elements in array should be Capability type
	for {
		var cap Capability

		// Decode the Capability from the list item
		// if error means the end of the list we're done
		// if not then oh-oh spaghettio's
		err := s.Decode(&cap)
		if err != nil {
			if err == rlp.EOL {
				break
			}
			return err
		}

		// Add the entry to the Capabilities array
		c.add(&cap)
	}

	return nil
}
