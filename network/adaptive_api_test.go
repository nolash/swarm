package network

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/rpc"
)

// TestCapabilitiesAPINotifications tests that API calls generates the expected notifications on subscriptions
func TestCapabilitiesAPINotifications(t *testing.T) {

	// Initialize capability
	caps := NewCapabilities(nil)

	// Set up faux rpc
	rpcSrv := rpc.NewServer()
	rpcClient := rpc.DialInProc(rpcSrv)
	rpcSrv.RegisterName("cap", NewCapabilitiesAPI(caps))

	// Subscribe to notifications
	changeRemoteC := make(chan CapabilityNotification)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := rpcClient.Subscribe(ctx, "cap", changeRemoteC, "subscribeChanges")
	if err != nil {
		t.Fatalf("Capabilities change subscription fail: %v", err)
	}

	// Catch capabilities change notifications in separate thread
	errRemoteC := make(chan error)
	go func() {
		i := 0
		for {
			select {
			case c, ok := <-changeRemoteC:
				if !ok {
					close(errRemoteC)
					return
				}
				// fail on unexpected id or flags
				if c.Id != 1 {
					errRemoteC <- fmt.Errorf("subscribe remote return wrong id, got: %d, expected %d", c.Id, 1)
				}
				if !bytes.Equal(c.Flags, expects[i]) {
					errRemoteC <- fmt.Errorf("subscribe remote return fail, got: %v, expected %v", c.Flags, expects[i])
				}
			}
			i = i + 1
		}
	}()

	// register capability
	err = rpcClient.Call(nil, "cap_registerCapabilityModule", 1, 2)
	if err != nil {
		t.Fatalf("RegisterCapabilityModule fail: %v", err)
	}

	// SetCapability with correct flag byte and capability id should succeed
	err = rpcClient.Call(nil, "cap_setCapability", 1, changes[0])
	if err != nil {
		t.Fatalf("SetCapability (1) fail: %v", err)
	}

	// Consecutive SetCapability should only set specified bytes, leave others alone
	err = rpcClient.Call(nil, "cap_setCapability", 1, changes[1])
	if err != nil {
		t.Fatalf("SetCapability (2) fail: %v", err)
	}

	// Removecapability should only remove specified bytes, leave others alone
	err = rpcClient.Call(nil, "cap_removeCapability", 1, changes[2])
	if err != nil {
		t.Fatalf("RemoveCapability fail: %v", err)
	}

	// check if notify listener recorded an anomaly and fail if it did
	sub.Unsubscribe()
	close(changeRemoteC)
	err, ok := <-errRemoteC
	if ok {
		t.Fatal(err)
	}
}
