//protoc -I streaming_sort/ streaming_sort/streaming_sort.proto --go_out=plugins=grpc:streaming_sort

package main

import (
	"flag"
	ss "github.com/vparonov/streaming/server"
	"log"
)

var (
	host = flag.String("h", "localhost", "server host")
	port = flag.Int("p", 10000, "server port")
)

func main() {
	flag.Parse()
	log.Printf("Starting server at %s:%d", *host, *port)

	err := ss.StartServer(*host, *port)

	if err != nil {
		log.Fatal(err)
	}
	log.Print("Bye!\n")
}
