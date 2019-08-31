package queue

import (
	"io"
	"os"
	"sync"
)

type Queue struct {
	file    *os.File
	rwMutex sync.RWMutex
}

type Element struct {
	size int64
	msg  uintptr
}

const ElementMetadataSize = 8

func (q *Queue) Push(bytes []byte) error {
	size := int64(len(bytes))

	data := append(int64ToBytes(size), bytes...)

	q.rwMutex.Lock()
	defer q.rwMutex.Unlock()

	_, err := q.file.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (q *Queue) Pop(offset int64) (newOffset int64, msg []byte, err error) {
	sizeBytes := make([]byte, ElementMetadataSize)

	// TODO mix read at
	q.rwMutex.RLock()
	_, err = q.file.ReadAt(sizeBytes, offset)
	if err != nil {
		if err == io.EOF {
			q.rwMutex.RUnlock()
			return offset, nil, err
		}
		q.rwMutex.RUnlock()
		return 0, nil, err
	}
	q.rwMutex.RUnlock()

	size := bytesToInt64(sizeBytes)

	data := make([]byte, size)

	_, err = q.file.ReadAt(data, offset+ElementMetadataSize)
	if err != nil {
		return 0, nil, err
	}

	return offset + ElementMetadataSize + size, data, nil
}

func (q *Queue) Close() error {
	return q.file.Close()
}

func (q *Queue) NewConsumer() *Consumer {
	return &Consumer{
		queue:    q,
		consumAt: 0,
	}
}
