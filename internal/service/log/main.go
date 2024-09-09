package logger

import (
	"fmt"
	"io"
	v1 "logger/gen/go/v1"
	filerepo "logger/internal/repository/file"
	"logger/internal/service/config"
	"logger/internal/service/segment"
	"logger/internal/transport/rpc"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrOffsetNotFound = fmt.Errorf("offset not found")
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config *config.Config

	activeSegment *segment.Segment
	segments      []*segment.Segment
}

// HARDCODE
// New creates a new log
func New(dir string, c *config.Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}

	// Read all existing segments
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// Parse the file names and keep only the offsets.
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	// Sort the offsets for the sake of determinism.
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// Create a new segment for each offset.
	for i := 0; i < len(baseOffsets); i++ {
		err := l.newSegment(baseOffsets[i])
		if err != nil {
			return nil, err
		}

		i++
	}

	// If segments are found, set the active segment to the last one.
	if l.segments == nil {
		err := l.newSegment(c.Segment.InitialOffset)
		if err != nil {
			return nil, err
		}
	}

	return l, nil
}

func (self *Log) Append(record *v1.Record) (uint64, error) {
	// Lock the log for writing.
	self.mu.Lock()
	defer self.mu.Unlock()

	fmt.Printf("Append: %+v\n", record)

	// Append the record to the active segment.
	off, err := self.activeSegment.Append(record)
	if err != nil {
		fmt.Println("Log Append err: ", err)
		return 0, err
	}

	fmt.Println("Append offset: ", off)
	// If the active segment is full, flush and create a new one.
	if self.activeSegment.IsMaxed() {
		err = self.newSegment(off + 1)
	}

	fmt.Println("Append err: ", err)
	return off, err
}

func (self *Log) Read(offset uint64) (*v1.Record, error) {
	self.mu.RLock()
	defer self.mu.RUnlock()

	// Find the segment that contains the record.
	var s *segment.Segment
	for _, segment := range self.segments {
		if segment.BaseOffset <= offset && offset < segment.NextOffset {
			s = segment
			break
		}
	}

	if s == nil || offset < s.BaseOffset {
		return nil, rpc.ErrOffsetOutOfRange{Offset: offset}
	}

	fmt.Println("Read offset: ", offset)
	return s.Read(offset)
}

// Close closes the log
func (self *Log) Close() error {
	fmt.Println("Log close")
	for _, segment := range self.segments {
		if err := segment.Close(); err != nil {
			fmt.Println("Segment close err: ", err)
			return err
		}
	}
	return nil
}

// Remove removes the log
func (self *Log) Remove() error {
	if err := self.Close(); err != nil {
		return err
	}
	return os.RemoveAll(self.Dir)
}

// Reset removes the log and creates a new one
func (self *Log) Reset() error {
	err := self.Remove()
	if err != nil {
		return err
	}

	newLog, err := New(self.Dir, self.Config)
	if err != nil {
		return err
	}

	self = newLog

	return nil
}

func (self *Log) LowestOffset() (uint64, error) {
	self.mu.RLock()
	defer self.mu.RUnlock()

	return self.segments[0].BaseOffset, nil
}

func (self *Log) HighestOffset() (uint64, error) {
	self.mu.RLock()
	defer self.mu.RUnlock()

	off := self.segments[len(self.segments)-1].NextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// Truncate removes all segments whose base offset is lower than lowest
// it is necessary because we don't have an infinite diskspace
func (self *Log) Truncate(lowest uint64) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	var segments []*segment.Segment
	for _, s := range self.segments {
		if s.NextOffset <= lowest+1 {
			err := s.Remove()
			if err != nil {
				return err
			}
			continue
		}

		segments = append(segments, s)
	}

	self.segments = segments

	return nil
}

// Reader returns an io.Reader to read the whole log
func (self *Log) Reader() io.Reader {
	self.mu.RLock()
	defer self.mu.RUnlock()

	readers := make([]io.Reader, len(self.segments))
	for i, segment := range self.segments {
		readers[i] = &OriginReader{segment.Store, 0}
	}
	return io.MultiReader(readers...)
}

// OriginReader is an io.Reader to read the whole log
type OriginReader struct {
	*filerepo.FileStorage
	off int64
}

// Read function implements the io.Reader interface
// for the OriginReader
func (self *OriginReader) Read(p []byte) (int, error) {
	n, err := self.ReadAt(p, self.off)
	self.off += int64(n)

	return n, err
}

// newSegment creates a new segment from a base offset and appends it to the log
func (self *Log) newSegment(baseOffset uint64) error {
	segment, err := segment.New(self.Dir, baseOffset, self.Config)
	if err != nil {
		return err
	}
	self.segments = append(self.segments, segment)
	self.activeSegment = segment
	return nil
}
