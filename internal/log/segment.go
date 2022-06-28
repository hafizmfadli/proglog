package log

import (
	"fmt"
	"os"
	"path"

	"github.com/golang/protobuf/proto"
	api "github.com/hafizmfadli/proglog/api/v1"
)

// segment wraps the index and store types to coordinate operations
// across the two. For example, when the log appends a record to the active segment,
// the segment needs to write the data to its store and add a new entry in the index.
// Similarly for reads, the segment needs to look up the entry from the index and then
// fetch the data from the store.
type segment struct {
	store *store
	index *index

	// we need the next and base offsets to know what offset to append new records under
	// and to calculate the relative offsets for the index entries
	baseOffset, nextOffset uint64
	config Config
}

// The log calls newSegment when it needs to add a new segment, such as when the current active segment
// hits its max size.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config: c,
	}
	var err error

	// Open store file and pass the os.O_CREATE file mode flag as an argument to os.OpenFile() to
	// create the files if they don't exist yet. os.O_APPEND flag to make the operating system append
	// to the file when writing.
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// create store instance using opened store file
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// Open index file using same technique as we open store file
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// create index instance using opened index file
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}


	if off, _, err := s.index.Read(-1); err != nil {
		// index is empty, then the next record appended to the segment
		// would be the first record and its offset would be the segment's base offset.
		s.nextOffset = baseOffset
	}else {
		// the index has at least one entry, then that means the offset the next record 
		// written should take the offset at the end of the segment, which we get by
		// adding 1 to the base offset and relative offset.
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append writes the record to the segment and returns the newly appended record's offset.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {	
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	// appends the data to the store
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// adds an index entry
	if err = s.index.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

// Read returns the record for the given offset.
func (s *segment) Read(off uint64) (*api.Record, error) {
	// First, translate the absolute index into a relative offset
	// and get associated index entry.
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// Once it has the index entry, the segment can go straight 
	// to the record's position in the store and read the proper amount
	// of data.
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// IsMaxed returns whether the segment has reached its max size,
// either by writing too much to the store or the index.
func (s *segment) IsMaxed() bool {
	// if you wrote a small number of long logs, then you'd hit the segment bytes limit.
	// if you wrote a lot of small logs, then you'd hit the index bytes limit.
	return s.store.size >= s.config.Segment.MaxStoreBytes || 
					s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove closes the segment and removes the index and store files.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the nearest and lesser multiple of k in j,
// for example nearestMultiple(9, 4) == 8. We take the lesser multiple
// to make sure we stay under the user's disk capacity
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}

