//protoc -I streaming_sort/ streaming_sort/streaming_sort.proto --go_out=plugins=grpc:streaming_sort

package server

import (
	"encoding/binary"
	"errors"
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

type dbConnection struct {
	db           *leveldb.DB
	guid         string
	putDataMutex sync.Mutex
	identity     uint64
}

type streamingSortServer struct {
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

func (s *streamingSortServer) BeginStream(ctx context.Context, dummy *pb.Empty) (*pb.StreamGuid, error) {
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

func (s *streamingSortServer) PutStreamData(ctx context.Context, putDataRequest *pb.PutDataRequest) (*pb.PutDataResponse, error) {

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

	return &pb.PutDataResponse{}, nil
}

func (s *streamingSortServer) PutStreamData2(stream pb.StreamingSort_PutStreamData2Server) error {
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

		return err
	}
}

func (s *streamingSortServer) GetSortedStream(streamGuid *pb.StreamGuid, stream pb.StreamingSort_GetSortedStreamServer) error {
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

func (s *streamingSortServer) EndStream(ctx context.Context, streamGuid *pb.StreamGuid) (*pb.EndStreamResponse, error) {
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

	delete(s.openDatabases, guid)

	return &pb.EndStreamResponse{}, nil
}

func newServer() *streamingSortServer {
	s := new(streamingSortServer)
	s.openDatabases = make(map[string]*dbConnection)
	return s
}

// private functions

func (s *streamingSortServer) removeDb(dbName string) error {
	return os.RemoveAll(dbName)
}

func (s *streamingSortServer) getConnection(guid string) (*dbConnection, error) {
	s.dbMutex.Lock()
	connection, found := s.openDatabases[guid]
	s.dbMutex.Unlock()

	if !found {
		retErr := errors.New("StreamGuid not found!")
		return nil, retErr
	}

	return connection, nil
}

func StartServer(host string, port int) error {
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
