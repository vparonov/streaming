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
	"sync/atomic"
)

var (
	port = flag.Int("port", 10000, "The server port")
)

type dbConnection struct {
	db           *leveldb.DB
	guid         string
	putDataMutex sync.Mutex
	identity     uint64
}

type streamingSortServerNode struct {
	openDatabases map[string]*dbConnection
	dbMutex       sync.Mutex
}

func newDbConnection(streamGuid string) (*dbConnection, error) {
	db, err := leveldb.OpenFile(streamGuid, nil)

	if err != nil {
		return nil, err
	}

	connection := new(dbConnection)
	connection.db = db
	connection.guid = streamGuid
	connection.identity = 0

	return connection, nil
}

func (s *streamingSortServerNode) BeginStream(ctx context.Context, dummy *pb.Empty) (*pb.StreamGuid, error) {
	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	streamGuid := uuid.NewV4().String()

	connection, err := newDbConnection(streamGuid)

	if err != nil {
		return nil, err
	}

	s.openDatabases[streamGuid] = connection

	m := new(pb.StreamGuid)
	m.Guid = streamGuid
	return m, nil
}

func (s *streamingSortServerNode) PutStreamData(ctx context.Context, putDataRequest *pb.PutDataRequest) (*pb.PutDataResponse, error) {

	connection, err := s.getConnection(putDataRequest.GetStreamID().GetGuid())

	if err != nil {
		return &pb.PutDataResponse{}, err
	}

	data := putDataRequest.GetData()

	batch := new(leveldb.Batch)
	for _, word := range data {
		key, value := connection.transformData(word)
		batch.Put(key, value)
	}

	err = connection.db.Write(batch, nil)

	if err != nil {
		return &pb.PutDataResponse{}, err
	}

	//batch.Put([]byte("foo"), []byte("value"))
	//batch.Put([]byte("bar"), []byte("another value"))
	//batch.Delete([]byte("baz"))
	//err = db.Write(batch, nil)

	//	for _, word := range data {
	//		err := s.put(connection, word)
	//		if err != nil {
	//			return &pb.PutDataResponse{}, err
	//		}
	//	}

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

		batch := new(leveldb.Batch)
		for _, word := range req.GetData() {
			key, value := connection.transformData(word)
			batch.Put(key, value)
		}

		err = connection.db.Write(batch, nil)

		if err != nil {
			return err
		}
	}
}

func (s *streamingSortServerNode) GetSortedStream(streamGuid *pb.StreamGuid, stream pb.StreamingSort_GetSortedStreamServer) error {
	connection, err := s.getConnection(streamGuid.GetGuid())

	if err != nil {
		return err
	}

	snapshot, err := connection.db.GetSnapshot()

	if err != nil {
		return err
	}

	defer snapshot.Release()

	iter := snapshot.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		var data string
		if len(key) > 0 {
			data = string(key[:len(key)-9])
		} else {
			data = ""
		}
		response := &pb.GetDataResponse{Data: data}
		err := stream.Send(response)
		if err != nil {
			return err
		}
	}
	return nil

}

func (s *streamingSortServerNode) EndStream(ctx context.Context, streamGuid *pb.StreamGuid) (*pb.EndStreamResponse, error) {
	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	guid := streamGuid.GetGuid()

	connection := s.openDatabases[guid]

	err := connection.db.Close()
	if err != nil {
		return nil, err
	}

	err = s.removeDb(guid)
	if err != nil {
		return nil, err
	}

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

func (connection *dbConnection) transformData(data string) ([]byte, []byte) {

	var emptyArray []byte
	key := []byte(data)

	buf := make([]byte, 8)

	connection.putDataMutex.Lock()

	atomic.AddUint64(&connection.identity, 1)
	binary.LittleEndian.PutUint64(buf, connection.identity)

	connection.putDataMutex.Unlock()

	key = append(key, 0)
	key = append(key, buf[:]...)

	return key, emptyArray
}

/*
func (s *streamingSortServerNode) put(connection *dbConnection, word string) error {
	key := []byte(word)
	buf := make([]byte, 8)

	connection.putDataMutex.Lock()
	defer connection.putDataMutex.Unlock()

	currentData, err := connection.db.Get(key, nil)

	if err == leveldb.ErrNotFound {
		binary.LittleEndian.PutUint64(buf, 1)
	} else {
		counter := binary.LittleEndian.Uint64(currentData)
		binary.LittleEndian.PutUint64(buf, counter+1)
	}

	err = connection.db.Put(key, buf, nil)

	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}
*/
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

func startServer(host string, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterStreamingSortServer(grpcServer, newServer())
	grpcServer.Serve(lis)

	return nil
}

func main() {
	flag.Parse()
	log.Printf("Starting server at localhost:%d", *port)

	err := startServer("localhost", *port)

	if err != nil {
		log.Fatal(err)
	}
	log.Print("Bye!\n")
}
