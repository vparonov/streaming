package main

import (
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"time"
)

var (
	word_length     = flag.Int("l", 50, "word length")
	number_of_words = flag.Int("n", 100000, "number of words")
	letterRunes     = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZабвгдежзийклмнопрстуфхцчшщьъюяАБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЪЮЯ一个笨蛋大蟾蜍")
)

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	flag.Parse()

	var lineDelimiter = "\n"
	if runtime.GOOS == "windows" {
		lineDelimiter = "\r\n"
	}

	for i := 0; i < *number_of_words; i++ {
		fmt.Printf("%s%s", RandStringRunes(*word_length), lineDelimiter)
	}

}
