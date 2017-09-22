package main

import (
	"bufio"
	"flag"
	sc "github.com/vparonov/streaming/client"
	"log"
	"os"
)

var (
	version        = flag.Int("v", 1, "runtime version (1 or 2)")
	serverAddr     = flag.String("s", "127.0.0.1:10000", "The server address in the format of host:port")
	inputFileName  = flag.String("i", "in.txt", "The input file name")
	outputFileName = flag.String("o", "out.txt", "The output file name")
	bufferSize     = flag.Int("b", 10000, "number of strings that are buffered")
)

func putData(client sc.StreamingSortClient, guid string, data []string, done chan int) {
	client.PutStreamData(guid, data)

	done <- 1
}

func main() {
	if *version == 1 {
		ver1()
	} else if *version == 2 {
		ver2()
	} else {
		log.Println("Version should be 1 or 2!")
	}
}

func ver2() {
	log.Println("Starting console client....")
	flag.Parse()

	client := sc.NewStreamingSortClient(*serverAddr)
	defer client.CloseConnection()

	guid, err := client.BeginStream()
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create(*outputFileName)
	defer f.Close()

	infile, err := os.Open(*inputFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer infile.Close()

	scanner := bufio.NewScanner(infile)

	log.Println("Reading input file")

	err = client.PutStreamData2(guid, scanner, *bufferSize)

	log.Println("Getting sorted stream")
	err = client.GetSortedStream(guid, f)

	if err != nil {
		log.Fatal(err)
	}

	err = client.EndStream(guid)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Bye!")
}

func ver1() {
	log.Println("Starting console client....")
	flag.Parse()

	client := sc.NewStreamingSortClient(*serverAddr)
	defer client.CloseConnection()

	guid, err := client.BeginStream()
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create(*outputFileName)
	defer f.Close()

	infile, err := os.Open(*inputFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer infile.Close()

	scanner := bufio.NewScanner(infile)

	bufsize := *bufferSize

	ar := make([]string, bufsize)

	ix := 0
	log.Println("Reading input file")

	done := make(chan int)
	procs := 0

	for scanner.Scan() {
		ar[ix] = scanner.Text()
		ix += 1
		if ix == bufsize {
			procs += 1
			go putData(client, guid, ar, done)
			ix = 0
		}
	}

	if ix > 0 {
		procs += 1
		go putData(client, guid, ar[0:ix], done)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	//
	log.Println("Waiting for go routines")

	for i := 0; i < procs; i++ {
		<-done
	}

	log.Println("Getting sorted stream")
	err = client.GetSortedStream(guid, f)

	if err != nil {
		log.Fatal(err)
	}

	err = client.EndStream(guid)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Bye!")
}
