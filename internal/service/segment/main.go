package segment

import (
	"fmt"
	v1 "logger/gen/go/v1"
	filerepo "logger/internal/repository/file"
	"logger/internal/service/config"
	"logger/internal/service/index"
	"os"
	"path"

	"google.golang.org/protobuf/proto"
)

// Segment defines a log segment
type Segment struct {
	// file storage
	Store *filerepo.FileStorage

	// log index
	index *index.Index

	// offset of the next log record
	BaseOffset uint64

	// offset of the next byte to be appended
	NextOffset uint64

	// max number of bytes in the segment
	config *config.Config
}

// New creates a new segment from a BaseOffset
func New(dir string, baseOffset uint64, c *config.Config) (*Segment, error) {
	s := &Segment{
		BaseOffset: baseOffset,
		config:     c,
	}

	// open the store file
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// New a file storage
	s.Store, err = filerepo.New(storeFile)
	if err != nil {
		return nil, err
	}

	// open the index file
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// New an index
	s.index, err = index.New(indexFile, c)
	if err != nil {
		return nil, err
	}

	// read the last offset from the index or set it to the BaseOffset
	if off, _, err := s.index.Read(-1); err != nil {
		s.NextOffset = baseOffset
	} else {
		s.NextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (self *Segment) Append(r *v1.Record) (offset uint64, err error) {
	cur := self.NextOffset
	r.Offset = cur
	fmt.Println("Marshal record: ", r)
	p, err := proto.Marshal(r)
	if err != nil {
		return 0, err
	}

	fmt.Println("Append record: ", p)
	_, pos, err := self.Store.Append(p)
	if err != nil {
		fmt.Println("Segment Append err: ", err)
		return 0, err
	}

	fmt.Println("Append pos: ", pos)
	fmt.Println("Append next offset: ", self.NextOffset)
	fmt.Println("Append base offset: ", self.BaseOffset)
	err = self.index.Write(uint32(self.NextOffset - uint64(self.BaseOffset)), pos)
	if err != nil {
		fmt.Println("Segment Write err: ", err)
		return 0, err
	}


	self.NextOffset++

	fmt.Println("Append next offset: ", self.NextOffset)
	return cur, nil
}

// Read reads a record from the segment
func (self *Segment) Read(off uint64) (*v1.Record, error) {
	_, pos, err := self.index.Read(int64(off - self.BaseOffset))
	if err != nil {
		return nil, err
	}

	p, err := self.Store.Read(pos)
	if err != nil {
		return nil, err
	}

	var record v1.Record
	err = proto.Unmarshal(p, &record)
	return &record, err
}

func (self *Segment) IsMaxed() bool {
	return self.Store.Size >= self.config.Segment.MaxStoreBytes ||
			self.index.Size >= self.config.Segment.MaxIndexBytes
}

func (self *Segment) Close() error {

	fmt.Println("Close index: ", self)
	if err := self.index.Close(); err != nil {
		return err
	}

	fmt.Println("Close store: ", self)
	if err := self.Store.Close(); err != nil {
		return err
	}

	return nil
}

func (self *Segment) Remove() error {
	err := self.Close()
	if err != nil {
		return err
	}

	err = os.Remove(self.Store.Name())
	if err != nil {
		return err
	}

	err = os.Remove(self.index.Name())
	if err != nil {
		return err
	}

	return nil
}

// NearestMultiple returns the nearest multiple of k
func NearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
