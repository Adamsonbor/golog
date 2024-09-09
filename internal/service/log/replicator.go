package logger

import (
	"context"
	"fmt"
	v1 "logger/gen/go/v1"
	"sync"

	"google.golang.org/grpc"
)

// Replicator replicates log entries to other nodes in the cluster.
type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer v1.LogClient

	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

func (self *Replicator) Join(name, addr string) error {
	// lock mutex to prevent race conditions
	self.mu.Lock()
	defer self.mu.Unlock()

	// initialize channels
	self.init()

	if self.closed {
		return nil
	}

	// check if server is already in map
	_, ok := self.servers[addr]
	if ok {
		return nil
	}

	// add server to map
	self.servers[addr] = make(chan struct{})

	go self.replicate(addr, self.servers[addr])

	return nil
}

// Replicate replicates log entries to other nodes in the cluster.
func (self *Replicator) replicate(addr string, leave chan struct{}) {
	// Create grpc client that connects to server
	cc, err := grpc.NewClient(addr, self.DialOptions...)
	if err != nil {
		self.err(err)
		return
	}
	// Close client when done
	defer cc.Close()

	// Create client to get stream from server
	client := v1.NewLogClient(cc)

	ctx := context.Background()

	// Get stream from server
	stream, err := client.ConsumeStream(
		ctx,
		&v1.ConsumeRequest{
			Offset: 0,
		},
	)
	if err != nil {
		self.err(err)
		return
	}

	// Get records from the stream
	records := make(chan *v1.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				self.err(err)
				return
			}

			records <- recv.Record
		}
	}()

	// Send records to the server
	for {
		select {
		case <-self.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err := self.LocalServer.Produce(
				ctx,
				&v1.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				self.err(err)
				return
			}
		}
	}
}

// Leave removes the server from the map.
func (self *Replicator) Leave(name, addr string) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.init()

	_, ok := self.servers[addr]
	if !ok {
		return nil
	}

	close(self.servers[addr])
	delete(self.servers, addr)

	return nil
}

// init initializes channels.
func (self *Replicator) init() {
	if self.servers == nil {
		self.servers = make(map[string]chan struct{})
	}

	if self.close == nil {
		self.close = make(chan struct{})
	}
}

// Close closes the replicator.
func (self *Replicator) Close() error {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.init()

	if self.closed {
		return nil
	}

	self.closed = true
	close(self.close)

	return nil
}

// Print log
func (self *Replicator) err(err error) {
	fmt.Printf("[ERROR] golog: %v\n", err)
}
