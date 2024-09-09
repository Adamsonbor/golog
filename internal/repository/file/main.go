package filerepo

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type FileStorage struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	Size uint64
}

func New(f *os.File) (*FileStorage, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())

	return &FileStorage{
		File: f,
		buf:  bufio.NewWriter(f),
		Size: size,
	}, nil
}

// Append function appends a record to the file
func (self *FileStorage) Append(p []byte) (n uint64, pos uint64, err error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	pos = self.Size

	// write the length of the record
	err = binary.Write(self.buf, enc, uint64(len(p)))
	if err != nil {
		fmt.Println("Storage write err: ", err)
		return 0, 0, err
	}

	// write the record
	w, err := self.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// update the number of bytes written
	w += lenWidth
	self.Size += uint64(w)
	return uint64(w), pos, nil
}

// Read function reads a record from the file
func (self *FileStorage) Read(pos uint64) ([]byte, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	err := self.buf.Flush()
	if err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)

	// read the length of the record
	_, err = self.File.ReadAt(size, int64(pos))
	if err != nil {
		return nil, err
	}

	// read the record
	body := make([]byte, enc.Uint64(size))
	_, err = self.File.ReadAt(body, int64(pos+lenWidth))
	if err != nil {
		return nil, err
	}

	// return the record
	return body, nil
}
