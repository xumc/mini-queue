package queue

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	queueProto "github.com/xumc/mini-queue/proto"
	"net"
	"sync"
)

type Client interface {
	Push(queueName string, data []byte) error
	Pop(queueName string) (data chan []byte, err error)
	Close() error
}

type client struct {
	conn          net.Conn
	writeConnChan chan []byte
	curID         int64
	curIDMutex    sync.Mutex

	recvChansMap   map[int64]chan []byte
	recvChansMutex sync.Mutex

	nextChansMap map[string]chan struct{} // queue_name => next chan
	nextChansMutex sync.Mutex
}

func NewClient(host string, port int32) (Client, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	client := &client{
		conn:           conn,
		writeConnChan:  make(chan []byte),
		curID:          0,
		curIDMutex:     sync.Mutex{},
		recvChansMap:   make(map[int64]chan []byte),
		recvChansMutex: sync.Mutex{},

		nextChansMap: make(map[string]chan struct{}),
		nextChansMutex: sync.Mutex{},
	}

	go func() {
		for {
			writeData := <-client.writeConnChan
			_, err := conn.Write(writeData)
			if err != nil {
				log.Fatalln(err)
			}
		}
	}()

	go func() {
		for {
			respBytes, err := readOneMsg(conn)
			if err != nil {
				log.Fatalln(err)
			}

			resp := &queueProto.Response{}
			if err := proto.Unmarshal(respBytes, resp); err != nil {
				log.Fatalln(err.Error())
			}

			switch resp.Op {
			case queueProto.Op_PUSH_ACK:
				client.nextChansMutex.Lock()
				client.nextChansMap[resp.Queue] <- struct{}{}
				log.Debug(resp.Id, "ACKED")
				client.nextChansMutex.Unlock()
			case queueProto.Op_POP_DATA:
				client.recvChansMutex.Lock()
				client.recvChansMap[resp.Id] <- resp.Data
				client.recvChansMutex.Unlock()
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


	nextChan, ok := c.nextChansMap[queueName]
	if !ok {
		nextChan =  make(chan struct{})

		c.nextChansMutex.Lock()
		c.nextChansMap[queueName] = nextChan
		c.nextChansMutex.Unlock()
	} else {
		<- nextChan
	}

	c.writeConnChan <- reqBytes

	return nil
}

func (c *client) Pop(queueName string) (data chan []byte, err error) {
	id := c.getMsgID()
	reqBytes, err := rawDataToRequestData(id, queueName, queueProto.Op_POP, []byte{})
	if err != nil {
		return nil, err
	}

	recvChan := make(chan []byte)
	c.recvChansMutex.Lock()
	c.recvChansMap[id] = recvChan
	c.writeConnChan <- reqBytes
	c.recvChansMutex.Unlock()

	return recvChan, nil
}

func (c *client) Close() error {
	return c.conn.Close()
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

	size := int64(len(pbBytes) + ElementMetadataSize)

	reqBytes := append(int64ToBytes(size), pbBytes...)
	return reqBytes, nil
}
