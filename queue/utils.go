package queue

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
)

func int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func bytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}
//
//func readMsg(conn io.Reader, callback func([]byte) error) error {
//	dataBytes := make([]byte, 0)
//	readSize := int64(-1)
//
//	for {
//		buffer := make([]byte, 1024)
//		n, err := conn.Read(buffer)
//		if err == io.EOF {
//			break
//		}
//
//		if err != nil {
//			log.Fatalln(err)
//			return err
//		}
//
//		if readSize == int64(-1) {
//			readSize = bytesToInt64(buffer[:elementMetadataSize])
//		}
//
//		dataBytes = append(dataBytes, buffer[:n]...)
//
//		if int64(len(dataBytes)) == readSize {
//			err = callback(dataBytes)
//			if err != nil {
//				return err
//			}
//
//			dataBytes = make([]byte, 0)
//			readSize = int64(-1)
//		}
//	}
//
//	return nil
//}

var ReaderEOFErr = errors.New("reader end of file")

func readOneMsg(conn io.Reader) ([]byte, error) {
	dataBytes := make([]byte, 0)
	readSize := int64(-1)

	for {
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalln(err)
			return nil, err
		}

		if readSize == int64(-1) {
			readSize = bytesToInt64(buffer[:elementMetadataSize])
		}

		dataBytes = append(dataBytes, buffer[:n]...)

		if int64(len(dataBytes)) == readSize {
			return dataBytes, nil
		}
	}

	return nil, ReaderEOFErr
}