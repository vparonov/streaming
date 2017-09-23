package client

import (
	"fmt"
	"io"
)

type ioWriterConsumer struct {
	writer io.Writer
}

func (c *ioWriterConsumer) Consume(data string) error {
	_, err := fmt.Fprintln(c.writer, data)
	return err
}

func NewIOWriterConsumer(w io.Writer) StreamingSortConsumer {
	return &ioWriterConsumer{w}
}
