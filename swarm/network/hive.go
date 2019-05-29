// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package network

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/swarm/log"
	"github.com/ethereum/go-ethereum/swarm/state"
)

/*
Hive is the logistic manager of the swarm

When the hive is started, a forever loop is launched that
asks the  kademlia nodetable
to suggest peers to bootstrap connectivity
*/

// HiveParams holds the config options to hive
type HiveParams struct {
	Discovery             bool  // if want discovery of not
	PeersBroadcastSetSize uint8 // how many peers to use when relaying
	MaxPeersPerRequest    uint8 // max size for peer address batches
	KeepAliveInterval     time.Duration
}

// NewHiveParams returns hive config with only the
func NewHiveParams() *HiveParams {
	return &HiveParams{
		Discovery:             true,
		PeersBroadcastSetSize: 3,
		MaxPeersPerRequest:    5,
		KeepAliveInterval:     500 * time.Millisecond,
	}
}

// Hive manages network connections of the swarm node
type Hive struct {
	*HiveParams                   // settings
	*Kademlia                     // the overlay connectiviy driver
	Store       state.Store       // storage interface to save peers across sessions
	addPeer     func(*enode.Node) // server callback to connect to a peer
	// bookkeeping
	lock   sync.Mutex
	peers  map[enode.ID]*BzzPeer
	ticker *time.Ticker
}

// NewHive constructs a new hive
// HiveParams: config parameters
// Kademlia: connectivity driver using a network topology
// StateStore: to save peers across sessions
func NewHive(params *HiveParams, kad *Kademlia, store state.Store) *Hive {
	return &Hive{
		HiveParams: params,
		Kademlia:   kad,
		Store:      store,
		peers:      make(map[enode.ID]*BzzPeer),
	}
}

// Start stars the hive, receives p2p.Server only at startup
// server is used to connect to a peer based on its NodeID or enode URL
// these are called on the p2p.Server which runs on the node
func (h *Hive) Start(server *p2p.Server) error {
	log.Info("Starting hive", "baseaddr", fmt.Sprintf("%x", h.BaseAddr()[:4]))

	// assigns the p2p.Server#AddPeer function to connect to peers
	h.addPeer = server.AddPeer

	// if state store is specified, load known peers to prepopulate the overlay address book, and
	// load prevous live peers and try to connect to them
	if h.Store != nil {
		log.Info("Detected an existing store. trying to load peers")
		if err := h.loadPeers(); err != nil {
			log.Error("hive error on load peers", "err", err)
			return err
		}
	}

	// ticker to keep the hive alive
	h.ticker = time.NewTicker(h.KeepAliveInterval)

	// this loop is doing bootstrapping and maintains a healthy table
	go h.connect()

	return nil
}

// Stop terminates the updateloop and saves the peers
func (h *Hive) Stop() error {
	log.Info("hive stopping, saving known and live peers")

	h.ticker.Stop()
	if h.Store != nil {
		if err := h.savePeers(); err != nil {
			return fmt.Errorf("could not save peers to persistence store: %v", err)
		}
		if err := h.Store.Close(); err != nil {
			return fmt.Errorf("could not close file handle to persistence store: %v", err)
		}
	}

	log.Debug("disconnecting peers")
	h.EachConn(nil, 255, func(p *Peer, _ int) bool {
		log.Debug("disconnecting peer", "peer", fmt.Sprintf("%x", p.Address()))
		p.Drop()
		return true
	})

	log.Debug("all peers disconnected")
	return nil
}

// connect is a forever loop
// at each iteration, ask the overlay driver to suggest the most preferred peer to connect to
// as well as advertises saturation depth if needed
func (h *Hive) connect() {
	for range h.ticker.C {
		addr, depth, changed := h.SuggestPeer()
		if h.Discovery && changed {
			NotifyDepth(uint8(depth), h.Kademlia)
		}
		if addr == nil {
			continue
		}

		log.Trace(fmt.Sprintf("%08x hive connect() suggested %08x", h.BaseAddr()[:4], addr.Address()[:4]))
		under, err := enode.ParseV4(string(addr.Under()))
		if err != nil {
			log.Warn(fmt.Sprintf("%08x unable to connect to bee %08x: invalid node URL: %v", h.BaseAddr()[:4], addr.Address()[:4], err))
			continue
		}
		log.Trace(fmt.Sprintf("%08x attempt to connect to bee %08x", h.BaseAddr()[:4], addr.Address()[:4]))
		h.addPeer(under)
	}
}

// Run protocol run function
func (h *Hive) Run(p *BzzPeer) error {
	h.trackPeer(p)
	defer h.untrackPeer(p)

	dp := NewPeer(p, h.Kademlia)
	depth, changed := h.On(dp)
	// if we want discovery, advertise change of depth
	if h.Discovery {
		if changed {
			// if depth changed, send to all peers
			NotifyDepth(depth, h.Kademlia)
		} else {
			// otherwise just send depth to new peer
			dp.NotifyDepth(depth)
		}
		NotifyPeer(p.BzzAddr, h.Kademlia)
	}
	defer h.Off(dp)
	return dp.Run(dp.HandleMsg)
}

func (h *Hive) trackPeer(p *BzzPeer) {
	h.lock.Lock()
	h.peers[p.ID()] = p
	h.lock.Unlock()
}

func (h *Hive) untrackPeer(p *BzzPeer) {
	h.lock.Lock()
	delete(h.peers, p.ID())
	h.lock.Unlock()
}

// NodeInfo function is used by the p2p.server RPC interface to display
// protocol specific node information
func (h *Hive) NodeInfo() interface{} {
	return h.String()
}

// PeerInfo function is used by the p2p.server RPC interface to display
// protocol specific information any connected peer referred to by their NodeID
func (h *Hive) PeerInfo(id enode.ID) interface{} {
	p := h.Peer(id)

	if p == nil {
		return nil
	}
	addr := NewAddr(p.Node())
	return struct {
		OAddr hexutil.Bytes
		UAddr hexutil.Bytes
	}{
		OAddr: addr.OAddr,
		UAddr: addr.UAddr,
	}
}

// Peer returns a bzz peer from the Hive. If there is no peer
// with the provided enode id, a nil value is returned.
func (h *Hive) Peer(id enode.ID) *BzzPeer {
	h.lock.Lock()
	defer h.lock.Unlock()

	return h.peers[id]
}

// loadPeers loads the persisted peer information on boot up
func (h *Hive) loadPeers() error {
	var livePeers []*BzzAddr

	err := h.Store.Get("livePeers", &livePeers)
	if err != nil {
		if err != state.ErrNotFound {
			return err
		}
		log.Debug("hive no persisted live peers found")
	}

	for _, p := range livePeers {
		log.Debug("connecting to persisted live peer", "peer", p)

		n, err := enode.ParseV4(string(p.UAddr))
		if err != nil {
			log.Error(err.Error())
		}

		h.addPeer(n)
	}

	var knownPeers []*BzzAddr
	err = h.Store.Get("knownPeers", &knownPeers)
	if err != nil {
		if err != state.ErrNotFound {
			return err
		}
		log.Debug("hive no persisted known peers found")
	}

	err = h.Register(knownPeers...)
	if err != nil {
		log.Error(err.Error())
	}

	return nil
}

// savePeers iterates over known and live peers and persists them so that we keep the state for next boot up
func (h *Hive) savePeers() error {
	var knownPeers []*BzzAddr
	var livePeers []*BzzAddr

	// iterate over known peers
	h.Kademlia.EachAddr(nil, 256, func(pa *BzzAddr, i int) bool {
		if pa == nil {
			log.Warn("hive empty addr", "addr", i)
			return true
		}
		log.Trace("saving known peer", "peer", pa)
		knownPeers = append(knownPeers, pa)
		return true
	})

	// iterate over live peers
	h.Kademlia.EachConn(nil, 256, func(p *Peer, i int) bool {
		if p == nil {
			log.Warn("hive empty addr", "addr", i)
			return true
		}
		log.Trace("saving live peer", "peer", p)
		livePeers = append(livePeers, p.BzzPeer.BzzAddr)
		return true
	})

	if err := h.Store.Put("livePeers", livePeers); err != nil {
		return fmt.Errorf("could not save live peers: %v", err)
	}

	if err := h.Store.Put("knownPeers", knownPeers); err != nil {
		return fmt.Errorf("could not save known peers: %v", err)
	}
	return nil
}
