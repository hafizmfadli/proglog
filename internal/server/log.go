package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mu sync.Mutex
	records []Record
}

func NewLog() *Log{
	return &Log{}
}

// Append a record to the log.
func (c *Log) Append(record Record) (uint64, error){
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

// Read a record given an index. If the offset given by the client
// doesn't exist, we return an error saying that the offset doesn't exist.
func (c *Log) Read(offset uint64) (Record, error){
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)){
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")
