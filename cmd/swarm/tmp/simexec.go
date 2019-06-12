package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethersphere/swarm/network/simulation"
)

const (
	serviceName = "foo"
)

var (
	serviceMap = make(map[string]simulation.ServiceFunc)
)

func init() {
	serviceMap[serviceName] = newMiniService
	hs := log.StreamHandler(os.Stderr, log.TerminalFormat(true))
	hf := log.LvlFilterHandler(log.Lvl(log.LvlTrace), hs)
	h := log.CallerFileHandler(hf)
	log.Root().SetHandler(h)
}

func main() {

	// set up basedir for execnode
	// since this is empty while running suspect this is for compile purposes only
	// look for dir swarm-sim* for node data dirs
	baseDir, err := ioutil.TempDir("", "swarm-test")
	if err != nil {
		panic(err)
	}
	log.Info("using basedir", "dir", baseDir)
	defer os.RemoveAll(baseDir)

	// create exec node in simulation
	net, err := simulation.NewExec(serviceMap)
	if err != nil {
		panic(err)
	}
	defer net.Close()

	// add two nodes to the newtork
	// AddNodes also STARTS the node services
	opt_services := []simulation.AddNodeOption{
		simulation.AddNodeWithService("foo"),
		simulation.AddNodeWithMsgEvents(true),
	}
	_, err = net.AddNodes(2, opt_services...)
	if err != nil {
		panic(err)
	}

	// subscribe to events from the network
	// these include node start, stop, connect, disconnect and messages
	// we are only interested in messages for this demonstration
	// messages are very verbose my default so we make sure we only get the ones from our protocol
	onlyProtoMsgFilter := simulation.NewPeerEventsFilter().ReceivedMessages().Protocol("foo")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	eventC := net.PeerEvents(ctx, net.NodeIDs(), onlyProtoMsgFilter)

	// connect the nodes
	err = net.Net.ConnectNodesFull(net.NodeIDs())
	if err != nil {
		panic(err)
	}

	// make both nodes send a message to each other
	// we use our provided RPC call to trigger it
	c := 0
	go func(cancel func()) {
		for _, n := range net.Net.GetNodes() {
			client, err := n.Client()
			if err != nil {
				cancel()
			}
			err = client.Call(nil, "foo_emit")
			if err != nil {
				cancel()
			}
		}
	}(cancel)

	// listen to the network events
	// we should be getting two message events
OUTER:
	for {
		select {
		case ev := <-eventC:
			fmt.Println(ev.Event)
			if ev.Event.Type == simulations.EventTypeMsg {
				c++
			}
			if c == 2 {
				break OUTER
			}
		case <-ctx.Done():
			log.Error("ev timeout", "err", ctx.Err())
			break OUTER
		}
	}
}

// FooApi defines the rpc API for this demo service
type FooApi struct {
	triggerC chan struct{}
}

func (f *FooApi) Emit() error {
	log.Info("got emit")
	f.triggerC <- struct{}{}
	return nil
}

// miniService implements node.Service
type miniService struct {
	triggerC chan struct{}
}

// NewAPI create the FooApi object with the trigger channel used by the protocol loop
func (m miniService) NewAPI() rpc.API {
	return rpc.API{
		Namespace: "foo",
		Version:   "0.1",
		Service: &FooApi{
			triggerC: m.triggerC,
		},
		Public: true,
	}
}

// run is the protocol loop function
func (m miniService) run(p *p2p.Peer, rw p2p.MsgReadWriter) error {
	go func() {
		select {
		case _, ok := <-m.triggerC:
			if !ok {
				fmt.Printf("croak %v\n", p)
				return
			}
			err := rw.WriteMsg(p2p.Msg{
				Code:    0,
				Size:    3,
				Payload: bytes.NewReader([]byte("bar")),
			})
			log.Info("sent msg", "p", p)
			if err != nil {
				panic(err)
			}
		}
	}()
	for {
		log.Info("got msg", "p", p)
		_, err := rw.ReadMsg()
		if err != nil {
			return err
		}
	}
}

// newMiniService is the simulation.ServiceFunc for our demonstration service
func newMiniService(ctx *adapters.ServiceContext, bucket *sync.Map) (s node.Service, cleanup func(), err error) {
	return &miniService{
		triggerC: make(chan struct{}),
	}, func() {}, nil
}

// APIs implements node.Service
func (m miniService) APIs() []rpc.API {
	return []rpc.API{
		m.NewAPI(),
	}
}

// Protocols implements node.Service
func (m miniService) Protocols() []p2p.Protocol {
	return []p2p.Protocol{
		{
			Name:    "foo",
			Version: 1,
			Length:  1,
			Run:     m.run,
		},
	}
}

// Start implements node.Service
func (m miniService) Start(server *p2p.Server) error {
	return nil
}

// Stop implements node.Service
func (m miniService) Stop() error {
	return nil
}
