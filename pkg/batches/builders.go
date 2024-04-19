package batches

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/dynamodb-verse/pkg/foundations"
	"github.com/goccha/dynamodb-verse/pkg/foundations/options"
	"github.com/pkg/errors"
)

const (
	MaxWriteItems = 25
)

type WriteItemFunc func() (table string, item map[string]types.AttributeValue, err error)

func PutItems(items ...foundations.WriteItemFunc) []WriteItemFunc {
	res := make([]WriteItemFunc, 0, len(items))
	for _, v := range items {
		f := v
		res = append(res, func() (table string, item map[string]types.AttributeValue, err error) {
			table, item, _, err = f()
			return
		})
	}
	return res
}

func DeleteItems(items ...foundations.WriteItemFunc) []WriteItemFunc {
	return PutItems(items...)
}

type writeItem struct {
	items map[string][]types.WriteRequest
	size  int
}

func (bi *writeItem) Size() int {
	return bi.size
}
func (bi *writeItem) Put(table string, req types.WriteRequest) (item *writeItem, newItem bool) {
	var items []types.WriteRequest
	item = bi
	if v, ok := item.items[table]; ok {
		if bi.size < MaxWriteItems {
			item.items[table] = append(v, req)
		} else {
			item = &writeItem{
				items: map[string][]types.WriteRequest{},
				size:  0,
			}
			item.Put(table, req)
			return item, true
		}
	} else {
		items = make([]types.WriteRequest, 0, MaxWriteItems)
		item.items[table] = append(items, req)
	}
	item.size++
	return item, false
}
func (bi *writeItem) run(ctx context.Context, cli WriteClient, opt ...options.Option) (err error) {
	body := bi.items
	for len(body) > 0 {
		var out *dynamodb.BatchWriteItemOutput
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: body,
		}
		for _, f := range opt {
			input = f(input).(*dynamodb.BatchWriteItemInput)
		}
		out, err = cli.BatchWriteItem(ctx, input)
		if err != nil {
			return errors.WithStack(err)
		}
		body = out.UnprocessedItems // 未処理のアイテム
	}
	return nil
}

type WriteClient interface {
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

type Monitor func(items map[string][]types.WriteRequest, err error) error

func New() *Builder {
	return &Builder{
		items: []*writeItem{},
	}
}

type Builder struct {
	items   []*writeItem
	err     error
	monitor Monitor
}

func (builder *Builder) Monitor(monitor Monitor) *Builder {
	builder.monitor = monitor
	return builder
}

func (builder *Builder) monitoring(items map[string][]types.WriteRequest, err error) error {
	if builder.monitor != nil {
		return builder.monitor(items, err)
	}
	return err
}

func (builder *Builder) HasError() bool {
	return builder.err != nil
}

func (builder *Builder) Put(items ...WriteItemFunc) *Builder {
	if builder.err != nil {
		return builder
	}
	for _, v := range items {
		if table, item, err := v(); err != nil {
			builder.err = err
			return builder
		} else {
			if it, newItem := builder.get(len(items)).Put(table, types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: item,
				},
			}); newItem {
				builder.items = append(builder.items, it)
			}
		}
	}
	return builder
}

func (builder *Builder) Delete(items ...WriteItemFunc) *Builder {
	if builder.err != nil {
		return builder
	}
	for _, v := range items {
		if table, item, err := v(); err != nil {
			builder.err = err
			return builder
		} else {
			if it, newItem := builder.get(len(items)).Put(table, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: item,
				},
			}); newItem {
				builder.items = append(builder.items, it)
			}
		}
	}
	return builder
}

func (builder *Builder) get(length int) *writeItem {
	var bi *writeItem
	index := len(builder.items) - 1
	if index < 0 {
		builder.items = make([]*writeItem, 0, length)
		bi = &writeItem{
			items: make(map[string][]types.WriteRequest),
		}
		builder.items = append(builder.items, bi)
	} else {
		bi = builder.items[index]
	}
	return bi
}

func (builder *Builder) Run(ctx context.Context, cli WriteClient, opt ...options.Option) (err error) {
	if builder.err != nil {
		return builder.err
	}
	for _, v := range builder.items {
		if err = v.run(ctx, cli, opt...); err != nil {
			if err = builder.monitoring(v.items, err); err != nil {
				return err
			}
		}
		if err = builder.monitoring(v.items, err); err != nil {
			return err
		}
	}
	return nil
}
