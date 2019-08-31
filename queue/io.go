package queue

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"io"
)

func readOneMsg(conn io.Reader) ([]byte, error) {
	sizeBytes := make([]byte, 0, ElementMetadataSize)

	for {
		if ElementMetadataSize == len(sizeBytes) {
			break
		}

		buffer := make([]byte, ElementMetadataSize- len(sizeBytes))
		n, err := conn.Read(buffer)
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalln(err)
			return nil, err
		}

		if len(sizeBytes) + n <= ElementMetadataSize {
			sizeBytes = append(sizeBytes, buffer[:n]...)
			continue
		} else {
			return nil, errors.New("read more than we need.")
		}
	}

	msgSize := bytesToInt64(sizeBytes)

	msgBodySize := msgSize - int64(ElementMetadataSize)

	msgBodyBytes := make([]byte, 0, msgBodySize)
	for {
		if msgBodySize == int64(len(msgBodyBytes)) {
			break
		}

		buffer := make([]byte, msgBodySize - int64(len(msgBodyBytes)))
		n, err := conn.Read(buffer)
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalln(err)
			return nil, err
		}

		if int64(len(msgBodyBytes) + n) <= msgBodySize {
			msgBodyBytes = append(msgBodyBytes, buffer[:n]...)
			continue
		} else {
			return nil, errors.New("read more than we need.")
		}
	}

	return msgBodyBytes, nil
}
