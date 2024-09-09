package index

import (
	"encoding/binary"
	"fmt"
	"io"
	"logger/internal/service/config"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

// Index defines an index for a log file
// It can be used to read and write
// and the position of the last entry
type Index struct {
	*os.File
	mmap gommap.MMap
	Size uint64
}

// New creates a new log index for the provided file
func New(f *os.File, c *config.Config) (*Index, error) {
	idx := &Index{
		File: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.Size = uint64(fi.Size())
	if err = os.Truncate(
		f.Name(),
		int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(
		idx.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

// Close closes the mmap
func (self *Index) Close() error {
	fmt.Println("mmap sync")
	// Sync the memory map changes to disk
	err := self.mmap.Sync(gommap.MS_SYNC)
	if err != nil {
		return err
	}

	fmt.Println("self sync")
	// Sync the file changes to disk
	err = self.File.Sync()
	if err != nil {
		return err
	}

	fmt.Println("truncate")
	// Truncate the file to the.Size of the index
	err = self.Truncate(int64(self.Size))
	if err != nil {
		return err
	}

	fmt.Println("close")
	// Close the file
	return self.File.Close()
}

// Read reads an entry from the index and return the offset and position
func (self *Index) Read(in int64) (out uint32, pos uint64, err error) {
	if self.Size == 0 {
		return 0, 0, io.EOF
	}

	// Find the offset for the given index
	if in == -1 {
		// Last index
		out = uint32(self.Size/entWidth - 1)
	} else {
		// The index
		out = uint32(in)
	}

	// Get the position
	pos = uint64(out) * entWidth
	// EOF if the position is beyond the end of the index
	if pos > self.Size {
		return 0, 0, io.EOF
	}

	out = binary.BigEndian.Uint32(self.mmap[pos : pos+offWidth])
	pos = binary.BigEndian.Uint64(self.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

// Write writes an entry to the index
func (self *Index) Write(off uint32, pos uint64) error {
	if uint64(len(self.mmap)) < self.Size+entWidth {
		fmt.Println("Not enough space in index")
		fmt.Println("Size: ", self.Size)
		fmt.Println("Offset: ", off)
		fmt.Println("Position: ", pos)
		fmt.Println("len mmap: ", len(self.mmap))
		return io.EOF
	}

	// Write the offset and position
	binary.BigEndian.PutUint32(self.mmap[self.Size:self.Size+offWidth], off)
	binary.BigEndian.PutUint64(self.mmap[self.Size+offWidth:self.Size+entWidth], pos)

	self.Size += entWidth

	return nil
}
