package batches

import (
	"context"
	"sync/atomic"

	"github.com/goccha/dynamodb-verse/pkg/foundations/options"
	"golang.org/x/sync/errgroup"
)

var MaxProcessorSize = 20

type BatchProcessor interface {
	Monitor(monitor Monitor) BatchProcessor
	Put(items ...WriteItemFunc) BatchProcessor
	Delete(items ...WriteItemFunc) BatchProcessor
	Run(ctx context.Context, cli WriteClient, opt ...options.Option) error
}

func NewProcessor(size ...int) BatchProcessor {
	if len(size) > 0 && size[0] > 1 {
		routines := size[0]
		if routines > MaxProcessorSize {
			routines = MaxProcessorSize
		}
		builders := make([]*Builder, routines)
		for i := 0; i < routines; i++ {
			builders[i] = New()
		}
		return &MultiProcessor{
			b:       builders,
			counter: -1,
		}
	}
	return &SingleProcessor{
		b: New(),
	}
}

type SingleProcessor struct {
	b *Builder
}

func (p *SingleProcessor) Monitor(monitor Monitor) BatchProcessor {
	p.b.Monitor(monitor)
	return p
}

func (p *SingleProcessor) Put(items ...WriteItemFunc) BatchProcessor {
	p.b.Put(items...)
	return p
}

func (p *SingleProcessor) Delete(items ...WriteItemFunc) BatchProcessor {
	p.b.Delete(items...)
	return p
}

func (p *SingleProcessor) Run(ctx context.Context, cli WriteClient, opt ...options.Option) error {
	return p.b.Run(ctx, cli, opt...)
}

type MultiProcessor struct {
	b       []*Builder
	counter int32
}

func (p *MultiProcessor) Monitor(monitor Monitor) BatchProcessor {
	for _, b := range p.b {
		b.Monitor(monitor)
	}
	return p
}

func (p *MultiProcessor) Put(items ...WriteItemFunc) BatchProcessor {
	size := len(p.b)
	i := atomic.AddInt32(&p.counter, 1)
	index := int(i) % size
	p.b[index].Put(items...)
	return p
}

func (p *MultiProcessor) Delete(items ...WriteItemFunc) BatchProcessor {
	size := len(p.b)
	i := atomic.AddInt32(&p.counter, 1)
	index := int(i) % size
	p.b[index].Delete(items...)
	return p
}

//type Errors []error
//
//func (err Errors) Error() string {
//	if len(err) > 0 {
//		return err[0].Error()
//	}
//	return ""
//}

func (p *MultiProcessor) Run(ctx context.Context, cli WriteClient, opt ...options.Option) error {
	eg, ctx := errgroup.WithContext(ctx)
	//wg := sync.WaitGroup{}
	for _, b := range p.b {
		//wg.Add(1)
		//go func(b *Builder) {
		//	defer wg.Done()
		//	if err := b.Run(ctx, cli, opt...); err != nil {
		//		return
		//	}
		//}(b)
		func(b *Builder) {
			eg.Go(func() error {
				return b.Run(ctx, cli, opt...)
			})
		}(b)
	}
	//wg.Wait()
	if err := eg.Wait(); err != nil {
		return err
	}
	//merr := Errors{}
	//for _, b := range p.b {
	//	if b.HasError() {
	//		merr = append(merr, b.err)
	//	}
	//}
	//if len(merr) > 0 {
	//	return merr
	//}
	return nil
}
