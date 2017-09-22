//protoc -I streaming_sort/ streaming_sort/streaming_sort.proto --go_out=plugins=grpc:streaming_sort
package main

import (
	"flag"
	"fmt"
	pb "github.com/vparonov/streaming/streaming_sort"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"os"
)

var (
	serverAddr = flag.String("server_addr", "127.0.0.1:10000", "The server address in the format of host:port")
)

func beginStream(client pb.StreamingSortClient) string {
	guid, err := client.BeginStream(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatal(err)
		return ""
	}
	return guid.GetGuid()
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

func getSortedStream(client pb.StreamingSortClient, guid string, output io.Writer) error {
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

		fmt.Fprintf(output, "%s\n", data.GetData())
	}

	return nil
}

func endStream(client pb.StreamingSortClient, guid string) {
	streamGuid := new(pb.StreamGuid)
	streamGuid.Guid = guid
	log.Println(*streamGuid)
	_, err := client.EndStream(context.Background(), streamGuid)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	log.Println("Starting broker")
	flag.Parse()
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewStreamingSortClient(conn)

	guid := beginStream(client)
	log.Printf("guid = %s\n", guid)

	data := []string{"A", "B", "C"}

	err = putStreamData(client, guid, data)

	if err != nil {
		log.Fatalf("error: %v", err)
	}

	err = getSortedStream(client, guid, os.Stdout)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	endStream(client, guid)

	log.Println("Bye!")
}
