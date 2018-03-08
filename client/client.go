//Package client: implements streaming sort client
//protoc -I streaming_sort/ streaming_sort/streaming_sort.proto --go_out=plugins=grpc:streaming_sort
package client

import (
	"bufio"
	"io"
	"log"

	pb "github.com/vparonov/streaming/streaming_sort"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type StreamingSortClient interface {
	BeginStream() (string, error)
	PutStreamData(string, []string) error
	PutStreamData2(string, *bufio.Scanner, int) error
	GetSortedStream(string, StreamingSortConsumer) error
	EndStream(string) error
	CloseConnection() error
}

type StreamingSortConsumer interface {
	Consume(string) error
}

type streamingSortClient struct {
	conn   *grpc.ClientConn
	client pb.StreamingSortClient
}

func NewStreamingSortClient(serverAddress string) StreamingSortClient {
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(serverAddress, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	client := pb.NewStreamingSortClient(conn)

	return &streamingSortClient{conn, client}
}

func (c *streamingSortClient) BeginStream() (string, error) {
	return beginStream(c.client)
}

func (c *streamingSortClient) PutStreamData(guid string, data []string) error {
	return putStreamData(c.client, guid, data)
}

func (c *streamingSortClient) PutStreamData2(guid string, reader *bufio.Scanner, bufferSize int) error {
	return putStreamData2(c.client, guid, reader, bufferSize)
}

func (c *streamingSortClient) GetSortedStream(guid string, consumer StreamingSortConsumer) error {
	return getSortedStream(c.client, guid, consumer)
}

func (c *streamingSortClient) EndStream(guid string) error {
	return endStream(c.client, guid)
}

func (c *streamingSortClient) CloseConnection() error {
	return c.conn.Close()
}

// implementation
func beginStream(client pb.StreamingSortClient) (string, error) {
	guid, err := client.BeginStream(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatal(err)
		return "", err
	}
	return guid.GetGuid(), nil
}

func putStreamData(client pb.StreamingSortClient, guid string, data []string) error {
	req := new(pb.PutDataRequest)
	sg := new(pb.StreamGuid)
	sg.Guid = guid

	req.StreamID = sg
	req.Data = data
	_, err := client.PutStreamData(context.Background(), req)
	return err
}

func putStreamData2(client pb.StreamingSortClient, guid string, input *bufio.Scanner, bufferSize int) error {

	req := new(pb.PutDataRequest2)
	req.StreamID = guid
	req.Data = make([]string, bufferSize)

	stream, err := client.PutStreamData2(context.Background())
	if err != nil {
		log.Fatalf("%v.putStreamData2(_) = _, %v", client, err)
	}

	i := 0
	ix := 0

	for input.Scan() {
		req.Data[ix] = input.Text()

		ix++

		if ix == bufferSize {
			if i == 1 {
				req.StreamID = ""
			}
			err := stream.Send(req)
			if err != nil {
				log.Fatalf("%v.Send(%v) = %v", stream, req, err)
			}

			i++
			ix = 0
		}
	}

	if ix > 0 {
		req.Data = req.Data[0:ix]
		if i == 1 {
			req.StreamID = ""
		}
		err := stream.Send(req)
		if err != nil {
			log.Fatalf("%v.Send(%v) = %v", stream, req, err)
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
	}

	return err
}

func getSortedStream(client pb.StreamingSortClient, guid string, consumer StreamingSortConsumer) error {
	sg := new(pb.StreamGuid)
	sg.Guid = guid

	stream, err := client.GetSortedStream(context.Background(), sg)

	if err != nil {
		log.Fatalf("%v.GetSortedStream(_) = _, %v", client, err)
		return err
	}

	for {
		data, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("%v.GetSortedStream(_) = _, %v", client, err)
			return err
		}
		consumer.Consume(data.GetData())
	}
	return nil
}

func endStream(client pb.StreamingSortClient, guid string) error {
	streamGuid := new(pb.StreamGuid)
	streamGuid.Guid = guid
	_, err := client.EndStream(context.Background(), streamGuid)
	if err != nil {
		log.Fatal(err)
	}
	return err
}
