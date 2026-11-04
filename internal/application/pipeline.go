package application

import "context"

type Processor interface {
	Once(context.Context) (bool, error)
}

// Pipeline checks durable queues in round-robin order. Construct one instance
// per worker goroutine; processors/repositories can safely be shared. A busy
// feedback queue must not starve indexing, or vice versa.
type Pipeline struct {
	Processors []Processor
	next       int
}

func (p *Pipeline) Once(ctx context.Context) (bool, error) {
	for checked := 0; checked < len(p.Processors); checked++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		processor := p.Processors[p.next]
		p.next = (p.next + 1) % len(p.Processors)
		worked, err := processor.Once(ctx)
		if worked || err != nil {
			return worked, err
		}
	}
	return false, nil
}
