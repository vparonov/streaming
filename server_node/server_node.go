//protoc -I streaming_sort/ streaming_sort/streaming_sort.proto --go_out=plugins=grpc:streaming_sort

package main

import (
	//"github.com/golang/protobuf/proto"
	"flag"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/twinj/uuid"
	pb "github.com/vparonov/streaming/streaming_sort"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
)

var (
	port = flag.Int("port", 10000, "The server port")
)

type streamingSortServerNode struct {
	openDataBases map[string](*leveldb.DB)
	putDataMutex  sync.Mutex
	getDataMutex  sync.RWMutex
	dbMutex       sync.Mutex
}

func (s *streamingSortServerNode) BeginStream(ctx context.Context, dummy *pb.Empty) (*pb.StreamGuid, error) {
	streamGuid := uuid.NewV4().String()
	m := new(pb.StreamGuid)
	m.Guid = streamGuid
	return m, nil
}

func (s *streamingSortServerNode) PutStreamData(ctx context.Context, putDataRequest *pb.PutDataRequest) (*pb.PutDataResponse, error) {
	return nil, nil
}

func (s *streamingSortServerNode) GetSortedStream(streamGuid *pb.StreamGuid, stream pb.StreamingSort_GetSortedStreamServer) error {
	return nil
}

func (s *streamingSortServerNode) EndStream(ctx context.Context, streamGuid *pb.StreamGuid) (*pb.EndStreamResponse, error) {
	return nil, nil
}

func newServer() *streamingSortServerNode {
	s := new(streamingSortServerNode)
	s.openDataBases = make(map[string](*leveldb.DB))
	return s
}

func main() {
	flag.Parse()
	log.Printf("Starting server at localhost:%d", *port)
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterStreamingSortServer(grpcServer, newServer())
	grpcServer.Serve(lis)

	log.Print("Bye!\n")
}
