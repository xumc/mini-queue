package queue

import "io"

type Consumer struct {
	consumAt int64
	queue    *Queue
}

func (c *Consumer) Pop() ([]byte, error) {
	for {
		newOffset, msg, err := c.queue.Pop(c.consumAt)
		if err != nil {
			if err == io.EOF {
				continue
			}
			return nil, err
		}

		c.consumAt = newOffset

		return msg, nil
	}
}

