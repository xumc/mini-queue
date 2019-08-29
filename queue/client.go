package queue

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	queueProto "github.com/xumc/mini-queue/queue/proto"
	"log"
	"net"
	"sync"
)

type Client interface {
	Push(queueName string, data []byte) error
	Close() error
	Pop(queueName string) (data chan []byte, err error)
}

type client struct {
	conn       net.Conn
	curID      int64
	curIDMutex sync.Mutex

	recvChan  map[int64]chan []byte
}

func NewClient(host string, port int32) (Client, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	client := &client{
		conn: conn,
		curID: 0,
		curIDMutex: sync.Mutex{},
		recvChan: make(map[int64]chan []byte),
	}

	go func() {
		for {
			respBytes, err := readOneMsg(client.conn)
			if err != nil {
				log.Fatalln(err)
			}

			resp := &queueProto.Response{}
			if err := proto.Unmarshal(respBytes[elementMetadataSize:], resp); err != nil {
				log.Fatalln(err.Error())
			}

			switch resp.Op {
			case queueProto.Op_POP_DATA:
				client.recvChan[resp.Id] <- resp.Data
			}

		}
	}()

	return client, nil
}

func (c *client) Push(queueName string, data []byte) error {
	id := c.getMsgID()
	reqBytes, err := rawDataToRequestData(id, queueName, queueProto.Op_PUSH, data)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(reqBytes)
	return err
}

func (c *client) Close() error {
	return c.conn.Close()
}

func (c *client) Pop(queueName string) (data chan []byte, err error) {
	id := c.getMsgID()
	reqBytes, err := rawDataToRequestData(id, queueName, queueProto.Op_POP, []byte{})
	if err != nil {
		return nil, err
	}

	recvChan := make(chan []byte)

	// TODO do we need lock the map
	c.recvChan[id] = recvChan

	_, err = c.conn.Write(reqBytes)
	if err != nil {
		return nil, err
	}

	return recvChan, nil
}

func (c *client) getMsgID() int64 {
	c.curIDMutex.Lock()
	defer c.curIDMutex.Unlock()
	c.curID++
	return c.curID
}

func rawDataToRequestData(id int64, queueName string, op queueProto.Op, data []byte) ([]byte, error) {
	pb := &queueProto.Request{
		Id:    id,
		Queue: queueName,
		Op:    op,
		Data:  data,
	}

	pbBytes, err := proto.Marshal(pb)
	if err != nil {
		return nil, err
	}

	size := int64(len(pbBytes) + elementMetadataSize)

	reqBytes := append(int64ToBytes(size), pbBytes...)
	return reqBytes, nil
}
