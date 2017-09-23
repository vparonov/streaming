package client

type stringArrayConsumer struct {
	array *[]string
}

func (c *stringArrayConsumer) Consume(data string) error {
	*c.array = append(*c.array, data)
	return nil
}

func NewStringArrayConsumer(a *[]string) StreamingSortConsumer {
	return &stringArrayConsumer{a}
}
