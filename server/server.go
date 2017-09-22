//protoc -I streaming_sort/ streaming_sort/streaming_sort.proto --go_out=plugins=grpc:streaming_sort

package main

import (
	//"github.com/golang/protobuf/proto"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/twinj/uuid"
	pb "github.com/vparonov/streaming/streaming_sort"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"os"
	"sync"
)

var (
	port = flag.Int("port", 10000, "The server port")
)

type dbConnection struct {
	db           *leveldb.DB
	guid         string
	putDataMutex sync.Mutex
	getDataMutex sync.RWMutex
}

type streamingSortServerNode struct {
	openDatabases map[string]*dbConnection
	dbMutex       sync.Mutex
}

func (s *streamingSortServerNode) BeginStream(ctx context.Context, dummy *pb.Empty) (*pb.StreamGuid, error) {
	streamGuid := uuid.NewV4().String()

	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	db, err := leveldb.OpenFile(streamGuid, nil)

	if err != nil {
		return nil, err
	}

	connection := new(dbConnection)
	connection.db = db
	connection.guid = streamGuid

	s.openDatabases[streamGuid] = connection

	m := new(pb.StreamGuid)
	m.Guid = streamGuid
	return m, nil
}

func (s *streamingSortServerNode) PutStreamData(ctx context.Context, putDataRequest *pb.PutDataRequest) (*pb.PutDataResponse, error) {

	s.dbMutex.Lock()
	connection, found := s.openDatabases[putDataRequest.GetStreamID().GetGuid()]
	s.dbMutex.Unlock()

	if !found {
		retErr := errors.New("StreamGuid not found!")
		return &pb.PutDataResponse{}, retErr
	}

	// TODO: add transaction semantic here
	for _, word := range putDataRequest.GetData() {
		err := s.put(connection, word)
		if err != nil {
			return &pb.PutDataResponse{}, err
		}
	}

	return &pb.PutDataResponse{}, nil
}

func (s *streamingSortServerNode) PutStreamData2(stream pb.StreamingSort_PutStreamData2Server) error {
	var connection *dbConnection

	for {
		req, err := stream.Recv()

		tmpGuid := req.GetStreamID()
		if tmpGuid != "" {
			connection, err = s.getConnection(tmpGuid)
			if err != nil {
				return err
			}
		}
		if err == io.EOF {
			return stream.SendAndClose(&pb.PutDataResponse{})
		}

		if err != nil {
			return err
		}

		for _, word := range req.GetData() {
			err := s.put(connection, word)
			if err != nil {
				return err
			}
		}
	}
}

func (s *streamingSortServerNode) GetSortedStream(streamGuid *pb.StreamGuid, stream pb.StreamingSort_GetSortedStreamServer) error {
	//log.Println("GetSortedStream")

	s.dbMutex.Lock()
	connection, found := s.openDatabases[streamGuid.GetGuid()]
	s.dbMutex.Unlock()

	if !found {
		retErr := errors.New("StreamGuid not found!")
		return retErr
	}

	connection.getDataMutex.Lock()
	defer connection.getDataMutex.Unlock()

	iter := connection.db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		data := string(key[:])
		n := binary.LittleEndian.Uint64(iter.Value())
		for i := uint64(0); i < n; i++ {
			response := &pb.GetDataResponse{Data: data}
			err := stream.Send(response)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *streamingSortServerNode) EndStream(ctx context.Context, streamGuid *pb.StreamGuid) (*pb.EndStreamResponse, error) {
	//log.Println("EndStream")

	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	guid := streamGuid.GetGuid()

	connection := s.openDatabases[guid]
	connection.db.Close()

	s.removeDb(guid)

	return &pb.EndStreamResponse{}, nil
}

func newServer() *streamingSortServerNode {
	s := new(streamingSortServerNode)
	s.openDatabases = make(map[string]*dbConnection)
	return s
}

// private functions

func (s *streamingSortServerNode) removeDb(dbName string) error {
	return os.RemoveAll(dbName)
}

func (s *streamingSortServerNode) put(connection *dbConnection, word string) error {
	key := []byte(word)
	buf := make([]byte, 8)

	connection.putDataMutex.Lock()
	defer connection.putDataMutex.Unlock()

	currentData, _ := connection.db.Get(key, nil)

	if currentData == nil {
		binary.LittleEndian.PutUint64(buf, 1)
	} else {
		counter := binary.LittleEndian.Uint64(currentData)
		binary.LittleEndian.PutUint64(buf, counter+1)
	}

	err := connection.db.Put(key, buf, nil)

	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

func (s *streamingSortServerNode) getConnection(guid string) (*dbConnection, error) {
	s.dbMutex.Lock()
	connection, found := s.openDatabases[guid]
	s.dbMutex.Unlock()

	if !found {
		retErr := errors.New("StreamGuid not found!")
		return nil, retErr
	}

	return connection, nil
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
