package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth = offWidth + posWidth
)

// index defines our index file, which comprises a persisted file and memory-mapped file.
// The size tell use the size of the index and where to write the next entry appended to the index.
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// How it works ?
// When we start our service, the service needs to know the offset to set on the next record
// appended to the log. The service learns the next record's offset by looking at
// the last entry of the index, a simple process of reading the last 12 bytes of the file.
// However, we mess up this process when we grow the files so we can memory-map them.
// (The reason we resize them now is that, once they're memory-mapped, we can't resize them, 
// so it's now or never). We grow the files by appending empty space at the end of them, so
// the last entry no longer at the end of the file (Instead there's some unknown amount of space
// between this entry and the file's end). This space prevents the service from restaring properly.
// That's why we shut down the service by truncating the index files to remove the empty space
// and put the last entry at the file once again. This graceful shutdown returns the service
// to the state where it can restart properly and efficiently

// newIndex creates and index for the given file.
func newIndex(f *os.File, c Config) (*index, error) {
	// create the index
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// save the current size of the file so we can track the amount of data in the index file
	// as we add index entries.
	idx.size = uint64(fi.Size())

	// Grow the file to the max index size before memory-mapping the file
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(idx.file.Fd(), 
	gommap.PROT_READ|gommap.PROT_WRITE,
	gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	return idx, nil
}

// Name returns the index's file path
func(i *index) Name() string {
	return i.file.Name()
}

// Read takes in an offset and returns the associated record's position in the store.
// The given offset is relative to the segment's base offset; 0 is always the offset
// of the index's first entry, 1 is the second entry, and so on. We use realtive offsets
// to reduce the size of the indexes by storing offsets as uint32s. If we used absolute
// offsets, we'd have to store the offsets as uint64s and require four more bytes for each entry.
func(i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	}else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos + offWidth : pos + entWidth])
	return out, pos, nil
}

// Write appends the given offset and position to the index.
func(i *index) Write(off uint32, pos uint64) error {
	// validate that we have space to write the entry
	if uint64(len(i.mmap)) < i.size + entWidth {
		return io.EOF
	}
	// encode the offset and position and write them to the memory-mapped
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

// Close makes sure the memory-mapped file has synced its data to the persisted file
// and that persisted file has flushed its contents to stable storage.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_ASYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}