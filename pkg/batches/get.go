package batches

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/dynamodb-verse/pkg/repositories"
)

const (
	MaxGetItems = 100
)

type getItem struct {
	keys map[string]types.KeysAndAttributes
	size int
}

func (gi getItem) Size() int {
	return gi.size
}
func (gi *getItem) Keys(table string, value map[string]types.AttributeValue, attrs []string) (key *getItem, newItem bool) {
	key = gi
	if v, ok := key.keys[table]; ok {
		if gi.size < MaxGetItems {
			key.keys[table] = types.KeysAndAttributes{
				Keys:            append(v.Keys, value),
				AttributesToGet: attrs,
			}
		} else {
			key = &getItem{
				keys: make(map[string]types.KeysAndAttributes),
				size: 0,
			}
			key.Keys(table, value, attrs)
			return key, true
		}
	} else {
		keys := make([]map[string]types.AttributeValue, 0, MaxGetItems)
		key.keys[table] = types.KeysAndAttributes{
			Keys:            append(keys, value),
			AttributesToGet: attrs,
		}
	}
	key.size++
	return key, false
}

func (gi *getItem) run(ctx context.Context, cli GetClient, fetch repositories.FetchItem) (err error) {
	keys := gi.keys
	for len(keys) > 0 {
		var out *dynamodb.BatchGetItemOutput
		out, err = cli.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: keys,
		})
		if err != nil {
			return fmt.Errorf("%w", err)
		}
		for table, values := range out.Responses {
			for _, v := range values {
				if err = fetch(table, v); err != nil {
					return err
				}
			}
		}
		keys = out.UnprocessedKeys
	}
	return nil
}

func Get(keys ...repositories.NewGetKey) *GetBuilder {
	builder := &GetBuilder{
		items: make([]*getItem, 0, len(keys)),
	}
	return builder.Keys(keys...)
}

type GetBuilder struct {
	items []*getItem
	err   error
}

func (builder *GetBuilder) HasError() bool {
	return builder.err != nil
}

func (builder *GetBuilder) Keys(keys ...repositories.NewGetKey) *GetBuilder {
	if builder.err != nil {
		return builder
	}
	for _, k := range keys {
		table, key, attr, err := k()
		if err != nil {
			builder.err = err
			return builder
		} else {
			if it, newItem := builder.get(len(keys)).Keys(table, key, attr); newItem {
				builder.items = append(builder.items, it)
			}
		}
	}
	return builder
}

func (builder *GetBuilder) get(length int) *getItem {
	var gi *getItem
	index := len(builder.items) - 1
	if index < 0 {
		builder.items = make([]*getItem, 0, length)
		gi = &getItem{
			keys: make(map[string]types.KeysAndAttributes),
		}
		builder.items = append(builder.items, gi)
	} else {
		gi = builder.items[index]
	}
	return gi
}

func (builder *GetBuilder) Run(ctx context.Context, cli GetClient, fetch repositories.FetchItem) (err error) {
	if builder.err != nil {
		return builder.err
	}
	for _, v := range builder.items {
		if err = v.run(ctx, cli, fetch); err != nil {
			return err
		}
	}
	return nil
}

type GetClient interface {
	BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
}
