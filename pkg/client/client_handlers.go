package client

import (
	"fmt"
	"go-cnc2/pkg/common"
	"go-cnc2/proto/pb"
	"io"
	"log"
	"net"

	logger "github.com/sirupsen/logrus"
)

func (c *Client) handleTcpStreamToConn(stream pb.ClientService_CreateTcpStreamClient, conn net.Conn) {
	for {
		pbChunk, err := stream.Recv()
		if err != nil {
			logger.Errorf(common.LogfmtErr, common.StrCliClient, common.StrHandleTcpStream, fmt.Sprintf(common.StrErrReadTcpStream, err))
			break
		}
		log.Printf("Client received %d bytes from stream, writing to SOCKS connection: % x", len(pbChunk.Data), pbChunk.Data)
		_, err = conn.Write(pbChunk.Data)
		if err != nil {
			logger.Errorf(common.LogfmtErr, common.StrCliClient, common.StrErrWriteTcpConn, err)
			break
		}
	}
}

func (c *Client) handleConnToTcpStream(conn net.Conn, stream pb.ClientService_CreateTcpStreamClient) {
	buff := make([]byte, 10000)
	for {
		n, err := conn.Read(buff)
		if err != nil {
			if err == io.EOF { // this happens occasionally
				break
			}
			if e, ok := err.(*net.OpError); ok {
				if e.Temporary() || e.Timeout() {
					// I don't think these actually happen, but we would want to continue if they did...
					continue
				} else if e.Err.Error() == "use of closed network connection" { // happens very frequently
					break
				}
			}
			if err.Error() != "use of closed network connection" {
				// not sure why this is different from the above similar case, but this one happens, also.
				log.Printf("read error: %v", err)
			}
			break
			//logger.Errorf(common.LogfmtErr, common.StrCliAgent, common.StrErrReadTcpConn, err)
			//continue
		}
		log.Printf("Client read %d bytes from SOCKS connection: % x", n, buff[:n])
		pbChunk := pb.Chunk{Data: buff[:n]}
		err = stream.Send(&pbChunk)
		if err != nil {
			logger.Errorf(common.LogfmtErr, common.StrCliClient, common.StrErrWriteTcpStream, err)
			break
		}
	}
}
