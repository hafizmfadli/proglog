package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// enc defines the encoding that we persist record sizes and index entries in
	enc = binary.BigEndian
)

const (
	// lenWidth defines the number of bytes used to store the record's length
	lenWidth = 8
)

type store struct {
	*os.File
	mu sync.Mutex
	buf *bufio.Writer
	size uint64
}

// newStore creates a store for the given file.
func newStore(f *os.File) (*store, error) {
	
	// get the file's current size. In case we're re-creating the store
	// from a file that has existing data, which would happend if, for example, our service had restarted.
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf: bufio.NewWriter(f),
	}, nil
}

// Append persists the given bytes to the store
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	
	// write the length of the record so that, when we read the record, we
	// know how many bytes to read. Uint64 takes 8 byte, So we will add additional
	// number of bytes written later. 
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// Add additional 8 byte
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read returns the record stored at the given position.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// First, flushes the writer buffer, in case we're about to try to read a reacord
	// that the buffer hansn't flushed to disk yet.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Find out how many bytes we have to read to get the whole record.
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// fetch the record
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt read len(p) bytes into p beginning at the off offset in the store's file.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close persists any buffered data before closing the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}