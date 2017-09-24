package client

import (
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
)

type ioWriterConsumer struct {
	writer        io.Writer
	firstLine     int32
	lineDelimiter string
}

func (c *ioWriterConsumer) Consume(data string) error {
	if c.firstLine == 0 {
		fmt.Fprint(c.writer, c.lineDelimiter)
	} else {
		atomic.StoreInt32(&c.firstLine, 0)
	}
	_, err := fmt.Fprint(c.writer, data)
	return err
}

func NewIOWriterConsumer(w io.Writer) StreamingSortConsumer {
	var lineDelimiter = "\n"
	if runtime.GOOS == "windows" {
		lineDelimiter = "\r\n"
	}
	return &ioWriterConsumer{w, 1, lineDelimiter}
}
