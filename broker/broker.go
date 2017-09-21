//protoc -I streaming_sort/ streaming_sort/streaming_sort.proto --go_out=plugins=grpc:streaming_sort
package main

import (
	"flag"
	pb "github.com/vparonov/streaming/streaming_sort"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
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
	endStream(client, guid)

	log.Println("Bye!")
}
