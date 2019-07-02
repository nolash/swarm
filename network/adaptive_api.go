package network

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethersphere/swarm/log"
)

// CapabilityNotification is a convenience wrapper for the capability bitvector
type CapabilityNotification struct {
	Id    uint8
	Flags []byte
}

// CapabilitiesAPI abstracts RPC API access to capabilities controls and provides notifications of capability changes
type CapabilitiesAPI struct {
	*Capabilities
	notifiers map[rpc.ID]*rpc.Notifier
}

// NetCapabilitiesAPI creates new API abstraction around provided Capabilities object
func NewCapabilitiesAPI(c *Capabilities) *CapabilitiesAPI {
	return &CapabilitiesAPI{
		Capabilities: c,
		notifiers:    make(map[rpc.ID]*rpc.Notifier),
	}
}

// SetCapability sets all set argument bits on the bitvector for the capability with corresponding id
// Argument must be of same length as the length given when module was registered
//
// For example:
// * bitvector is 0x10001000
// * argument is 0x00001001
// * bitvector becomes 0x10001001
func (a *CapabilitiesAPI) SetCapability(id uint8, flags []byte) error {
	return a.Capabilities.set(id, flags)
}

// RemoveCapability unsets all set argument bits on the bitvector for the capability with corresponding id
// Argument must be of same length as the length given when module was registered
//
// For example:
// * bitvector is 0x10000001
// * argument is 0x00000001
// * bitvector becomes 0x10000000
func (a *CapabilitiesAPI) RemoveCapability(id uint8, flags []byte) error {
	return a.Capabilities.unset(id, flags)
}

// RegisterCapabilityModule creates a new capability bitvector with the specified id
// The length argument specifies the length of the bitvector in bytes
//
// Fails if module with same id already exists
func (a *CapabilitiesAPI) RegisterCapabilityModule(id uint8, length uint8) error {
	return a.Capabilities.registerModule(id, length)
}

// SubscribeChanges creates a new subscription for changes in capabilities
//
// The notifications are represented by a CapabilityNotification struct
func (a CapabilitiesAPI) SubscribeChanges(ctx context.Context) (*rpc.Subscription, error) {
	notifier, ok := rpc.NotifierFromContext(ctx)
	if !ok {
		return nil, errors.New("notifications not supported")
	}
	sub := notifier.CreateSubscription()
	a.notifiers[sub.ID] = notifier
	go func(sub *rpc.Subscription, notifier *rpc.Notifier) {
		select {
		case err := <-sub.Err():
			log.Warn("rpc capabilities subscription end", "err", err)
		case <-notifier.Closed():
			log.Warn("rpc capabilities notifier closed")
		}
	}(sub, notifier)
	return sub, nil
}

// push change of capability notifications to subscribers
func (a CapabilitiesAPI) notify(c capability) {
	for id, notifier := range a.notifiers {
		notifier.Notify(id, &CapabilityNotification{
			Id:    uint8(c[0]),
			Flags: c[2:],
		})
	}
}
