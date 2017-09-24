package main

import (
	sc "github.com/vparonov/streaming/client"
	"log"
	"os"
	"sort"
	"testing"
)

func init() {
	go func() {
		err := startServer("localhost", 10000)

		if err != nil {
			log.Fatal(err)
		}
		log.Print("Bye!\n")
	}()
}

func TestBeginStream_EndStream(t *testing.T) {
	done := make(chan int)

	go func() {
		client := sc.NewStreamingSortClient("localhost:10000")
		defer client.CloseConnection()

		guid, err := client.BeginStream()
		if err != nil {
			t.Fail()
		}

		err = client.EndStream(guid)
		if err != nil {
			log.Fatal(err)
		}

		if _, err := os.Stat(guid); os.IsExist(err) {
			t.Error("Expected ", guid, " not to exists!")
		}

		done <- 1
	}()
	<-done
}

func TestSorting(t *testing.T) {
	done := make(chan int)

	go func() {
		client := sc.NewStreamingSortClient("localhost:10000")
		defer client.CloseConnection()

		guid, err := client.BeginStream()
		if err != nil {
			t.Fail()
		}

		inputArray := makeInputArray()

		sorted_array := make([]string, 0)

		consumer := sc.NewStringArrayConsumer(&sorted_array)

		err = client.PutStreamData(guid, inputArray)

		if err != nil {
			t.Fail()
		}

		err = client.GetSortedStream(guid, consumer)
		if err != nil {
			t.Fail()
		}

		sort.Strings(inputArray)

		//		for ix, s := range inputArray {
		//			log.Printf("%s -> %s\n", s, sorted_array[ix])
		//		}

		checkArrays(t, inputArray, sorted_array)

		err = client.EndStream(guid)
		if err != nil {
			t.Fail()
		}

		if _, err := os.Stat(guid); os.IsExist(err) {
			t.Error("Expected ", guid, " not to exists!")
		}

		done <- 1
	}()
	<-done
}

func TestConcurentCalls(t *testing.T) {
	done := make(chan int)

	go func() {
		client := sc.NewStreamingSortClient("localhost:10000")
		defer client.CloseConnection()

		guid, err := client.BeginStream()
		if err != nil {
			t.Fail()
		}

		inputArray := makeInputArray()

		sorted_array := make([]string, 0)

		consumer := sc.NewStringArrayConsumer(&sorted_array)

		err = client.PutStreamData(guid, inputArray)

		if err != nil {
			t.Fail()
		}

		err = client.GetSortedStream(guid, consumer)
		if err != nil {
			t.Fail()
		}

		sort.Strings(inputArray)

		checkArrays(t, inputArray, sorted_array)

		err = client.EndStream(guid)
		if err != nil {
			t.Fail()
		}

		if _, err := os.Stat(guid); os.IsExist(err) {
			t.Error("Expected ", guid, " not to exists!")
		}

		done <- 1
	}()
	<-done
}

// helper functions
func makeInputArray() []string {
	retval := []string{
		"A",
		"A",
		"A",
		"Zebra",
		"enable",
		"Enable",
		"Gushter",
		"Liberté",
		"égalité",
		"fraternité",
		"9oww",
		"_sdfsdf",
		"Голям",
		"Праз",
	}
	return retval
}

func checkArrays(t *testing.T, a []string, b []string) {
	l1 := len(a)
	l2 := len(b)
	if l1 != l2 {
		t.Error("Expected len = ", l1, " Actual len = ", l2)
	}

	for i, va := range a {
		if i < l2 {
			if va != b[i] {
				t.Error("Expected", va, " Actual ", b[i])
			}
		} else {
			t.Error("Expected", va, "  Missing ")
		}
	}
}
