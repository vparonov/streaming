//protoc -I streaming_sort/ streaming_sort/streaming_sort.proto --go_out=plugins=grpc:streaming_sort
package main

import (
	"flag"
	"fmt"
	//"github.com/olebedev/config"
	pb "github.com/vparonov/streaming/streaming_sort"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"math/rand"
	"os"
	"time"
)

var (
	serverAddr = flag.String("server_addr", "127.0.0.1:10000", "The server address in the format of host:port")
)

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

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

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func putSomeRandomData(client pb.StreamingSortClient, guid string, done chan int) {
	nstrings := 1
	strings := make([]string, nstrings)
	for i := 0; i < nstrings; i++ {
		strings[i] = RandStringRunes(50)
	}

	putStreamData(client, guid, strings)

	done <- 1
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

	var guids []string

	for i := 0; i < 5; i++ {
		guids = append(guids, beginStream(client))
	}

	//	log.Printf("guid = %s\n", guid)

	done := make(chan int)
	nprocs := 100

	for i := 0; i < nprocs; i++ {
		go putSomeRandomData(client, guids[i%5], done)
	}

	for i := 0; i < nprocs; i++ {
		<-done
	}

	if err != nil {
		log.Fatalf("error: %v", err)
	}

	for _, guid := range guids {
		log.Printf("dump of %s\n", guid)
		err = getSortedStream(client, guid, os.Stdout)
		if err != nil {
			log.Fatalf("error: %v", err)
		}
		endStream(client, guid)
	}

	log.Println("Bye!")
}
