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

func beginStream(client pb.StreamingSortClient) {
	guid, err := client.BeginStream(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("guid = %s", guid)
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

	beginStream(client)

	log.Println("Bye!")
}
