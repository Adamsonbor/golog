package discovery

import (
	"log"
	"net"

	"github.com/hashicorp/serf/serf"
)

/*
This package implements the Membership service
to manage the cluster members.
This is necessary for the distributed logging service.
It uses the serf package to manage the cluster members.
*/

// Handler is used to handle join and leave events
type Handler interface {
	Join(name, addr string) error
	Leave(name, addr string) error
}

// Membership manages the cluster members.
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
}

// Config is used to configure the Membership.
type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

// setupSerf creates the serf instance.
// listens for events and joins the cluster.
func (self *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", self.BindAddr)
	if err != nil {
		return err
	}

	// create serf instance with default config
	// and bind address and port
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	self.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	// Listen for events
	go self.eventHandler()

	// join the serf cluster with start join addrs
	// if it is not empty
	if self.StartJoinAddrs != nil {
		_, err = self.serf.Join(self.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// eventHandler listens for serf events
// and handles join and leave events
func (self *Membership) eventHandler() {
	for e := range self.events {
		switch e.EventType() {
		// handle member join event
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				// ignore local member
				if self.isLocal(member) {
					continue
				}
				// join the cluster
				self.handleJoin(member)
			}
		// handle member leave event
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				// ignore local member
				if self.isLocal(member) {
					continue
				}
				// leave the cluster
				self.handleLeave(member)
			}
		}
	}
}

func (self *Membership) handleJoin(member serf.Member) {
	err := self.handler.Join(member.Name, member.Tags["rpc_addr"])
	if err != nil {
		log.Printf(
			"[ERROR] golog: failed to join member %s: %s",
			member.Name,
			member.Tags["rpc_addr"],
		)
	}
}

func (self *Membership) handleLeave(member serf.Member) {
	err := self.handler.Leave(member.Name, member.Tags["rpc_addr"])
	if err != nil {
		log.Printf(
			"[ERROR] golog: failed to leave member %s: %s",
			member.Name,
			member.Tags["rpc_addr"],
		)
	}
}

func (self *Membership) isLocal(m serf.Member) bool {
	return m.Addr.String() == self.BindAddr
}

func (self *Membership) Members() []serf.Member {
	return self.serf.Members()
}

func (self *Membership) Leave() error {
	return self.serf.Leave()
}
