package batches

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/cloudflare/backoff"
	"github.com/goccha/dynamodb-verse/pkg/foundations"
	"github.com/pkg/errors"
)

func NewBatch[T any](tableName string, entities []T, opt ...Option) *Batch[T] {
	b := &Batch[T]{
		tableName: tableName,
		entities:  entities,
		option:    defaultBatchOption(),
	}
	for _, o := range opt {
		o(&b.option)
	}
	return b
}

type Batch[T any] struct {
	tableName string
	entities  []T
	err       error
	option    batchOption
}

func (builder *Batch[T]) HasError() bool {
	return builder.err != nil
}

func (builder *Batch[T]) Put(ctx context.Context, cli WriteClient) error {
	for i := 0; i < len(builder.entities); i += MaxWriteItems {
		end := i + MaxWriteItems
		if end > len(builder.entities) {
			end = len(builder.entities)
		}
		if err := batchWrite(ctx, cli, builder.tableName, builder.entities[i:end], builder.option); err != nil {
			return err
		}
	}
	return nil
}

func batchWrite[T any](ctx context.Context, cli WriteClient, tableName string, entities []T, opt batchOption) (err error) {
	if len(entities) > MaxWriteItems {
		return fmt.Errorf("batch write size is within %d items", MaxWriteItems)
	}
	items := make([]types.WriteRequest, 0, len(entities))
	for _, v := range entities {
		var av map[string]types.AttributeValue
		if av, err = attributevalue.MarshalMap(v); err != nil {
			return
		}
		items = append(items, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: av,
			},
		})
	}
	b := backoff.New(opt.maxInterval, opt.interval)
	i := 0
	for ; len(items) > 0 && i < opt.maxRetry; i++ {
		var out *dynamodb.BatchWriteItemOutput
		out, err = cli.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: items,
			},
		})
		if err != nil {
			return fmt.Errorf("batch write to %s: %w", tableName, err)
		}
		if len(out.UnprocessedItems[tableName]) > 0 {
			items = append(items[:0], out.UnprocessedItems[tableName]...) // スライスを初期化して未処理のitemsがあれば追加
			if len(items) > 0 {
				<-time.After(b.Duration())
			}
		} else {
			break
		}
	}
	b.Reset()
	if i >= opt.maxRetry {
		return fmt.Errorf("batch write to %s exceeded max retry limit", tableName)
	}
	return nil
}

type DeleteKeyFunc[T any] func(v T) map[string]types.AttributeValue

func (builder *Batch[T]) Delete(ctx context.Context, cli WriteClient, f ...DeleteKeyFunc[T]) error {
	for i := 0; i < len(builder.entities); i += MaxWriteItems {
		end := i + MaxWriteItems
		if end > len(builder.entities) {
			end = len(builder.entities)
		}
		var getDeleteKey DeleteKeyFunc[T]
		if len(f) > 0 {
			getDeleteKey = f[0]
		}
		if err := batchDelete(ctx, cli, builder.tableName, builder.entities[i:end], getDeleteKey, builder.option); err != nil {
			return err
		}
	}
	return nil
}

func batchDelete[T any](ctx context.Context, cli WriteClient, tableName string, entities []T, getDeleteKey DeleteKeyFunc[T], opt batchOption) (err error) {
	if len(entities) > MaxWriteItems {
		return fmt.Errorf("batch write size is within %d items", MaxWriteItems)
	}
	items := make([]types.WriteRequest, 0, len(entities))
	for _, v := range entities {
		var av map[string]types.AttributeValue
		if getDeleteKey != nil {
			av = getDeleteKey(v)
		} else {
			if av, err = attributevalue.MarshalMap(v); err != nil {
				return
			}
		}
		items = append(items, types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: av,
			},
		})
	}
	b := backoff.New(opt.maxInterval, opt.interval)
	i := 0
	for ; len(items) > 0 && i < opt.maxRetry; i++ {
		var out *dynamodb.BatchWriteItemOutput
		out, err = cli.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: items,
			},
		})
		if err != nil {
			return fmt.Errorf("batch delete to %s: %w", tableName, err)
		}
		if len(out.UnprocessedItems[tableName]) > 0 {
			items = append(items[:0], out.UnprocessedItems[tableName]...) // スライスを初期化して未処理のitemsがあれば追加
			if len(items) > 0 {
				<-time.After(b.Duration())
			}
		} else {
			break
		}
	}
	b.Reset()
	if i >= opt.maxRetry {
		return fmt.Errorf("batch write to %s exceeded max retry limit", tableName)
	}
	return nil
}

func (builder *Batch[T]) Get(ctx context.Context, cli GetClient, fetch foundations.FetchItemFunc) (err error) {
	for i := 0; i < len(builder.entities); i += MaxGetItems {
		end := i + MaxGetItems
		if end > len(builder.entities) {
			end = len(builder.entities)
		}
		if err = batchGet(ctx, cli, builder.tableName, builder.entities[i:end], fetch, builder.option); err != nil {
			return err
		}
	}
	return nil
}

func batchGet[T any](ctx context.Context, cli GetClient, tableName string, entities []T, fetch foundations.FetchItemFunc, opt batchOption) (err error) {
	if len(entities) > MaxGetItems {
		return fmt.Errorf("batch write size is within %d items", MaxGetItems)
	}
	attrs := make([]map[string]types.AttributeValue, 0, len(entities))
	var av map[string]types.AttributeValue
	for _, v := range entities {
		if av, err = attributevalue.MarshalMap(v); err != nil {
			return
		}
		attrs = append(attrs, av)
	}
	keys := map[string]types.KeysAndAttributes{}
	keys[tableName] = types.KeysAndAttributes{
		Keys: attrs,
	}
	b := backoff.New(opt.maxInterval, opt.interval)
	i := 0
	for ; len(keys) > 0 && i < opt.maxRetry; i++ {
		var out *dynamodb.BatchGetItemOutput
		out, err = cli.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: keys,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		for table, values := range out.Responses {
			for _, v := range values {
				if err = fetch(table, v); err != nil {
					return err
				}
			}
		}
		keys = out.UnprocessedKeys
		if len(keys) > 0 {
			<-time.After(b.Duration())
		}
	}
	b.Reset()
	if i >= opt.maxRetry {
		return fmt.Errorf("batch get to %s exceeded max retry limit", tableName)
	}
	return nil
}
