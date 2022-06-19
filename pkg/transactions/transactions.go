package transactions

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/dynamodb-verse/pkg/foundations"
	"time"
)

const (
	MaxItems = 25
)

type Transaction interface {
	PutItem(ctx context.Context, expiredAt ...time.Time) foundations.WriteItemFunc
	DeleteItem(ctx context.Context) foundations.WriteItemFunc
	UpdateItem(ctx context.Context, fields ...foundations.UpdateField) foundations.WriteItemFunc
}

func New() *Builder {
	return &Builder{
		items: make([]types.TransactWriteItem, 0, MaxItems),
	}
}

type Builder struct {
	items []types.TransactWriteItem
	err   error
}

func (builder *Builder) HasError() bool {
	return builder.err != nil
}

// Put 追加用
func (builder *Builder) Put(keys ...foundations.WriteItemFunc) *Builder {
	if builder.err != nil {
		return builder
	}
	if builder.items == nil {
		builder.items = make([]types.TransactWriteItem, 0, MaxItems)
	}
	items := make([]types.TransactWriteItem, 0, len(keys))
	for _, k := range keys {
		if table, item, expr, err := k(); err != nil {
			builder.err = err
			return builder
		} else {
			items = append(items, types.TransactWriteItem{
				Put: &types.Put{
					TableName:                 aws.String(table),
					Item:                      item,
					ExpressionAttributeNames:  expr.Names(),
					ExpressionAttributeValues: expr.Values(),
					ConditionExpression:       expr.Condition(),
				},
			})
		}
	}
	builder.items = append(builder.items, items...)
	return builder
}

// Delete 削除用
func (builder *Builder) Delete(keys ...foundations.WriteItemFunc) *Builder {
	if builder.err != nil {
		return builder
	}
	if builder.items == nil {
		builder.items = make([]types.TransactWriteItem, 0, MaxItems)
	}
	items := make([]types.TransactWriteItem, 0, len(keys))
	for _, k := range keys {
		if table, item, expr, err := k(); err != nil {
			builder.err = err
			return builder
		} else {
			items = append(items, types.TransactWriteItem{
				Delete: &types.Delete{
					TableName:                 aws.String(table),
					Key:                       item,
					ExpressionAttributeNames:  expr.Names(),
					ExpressionAttributeValues: expr.Values(),
					ConditionExpression:       expr.Condition(),
				},
			})
		}
	}
	builder.items = append(builder.items, items...)
	return builder
}

// Update 更新用
func (builder *Builder) Update(keys ...foundations.WriteItemFunc) *Builder {
	if builder.err != nil {
		return builder
	}
	if builder.items == nil {
		builder.items = make([]types.TransactWriteItem, 0, MaxItems)
	}
	items := make([]types.TransactWriteItem, 0, len(keys))
	for _, k := range keys {
		if table, item, expr, err := k(); err != nil {
			builder.err = err
			return builder
		} else {
			items = append(items, types.TransactWriteItem{
				Update: &types.Update{
					Key:                       item,
					TableName:                 aws.String(table),
					UpdateExpression:          expr.Update(),
					ExpressionAttributeValues: expr.Values(),
					ExpressionAttributeNames:  expr.Names(),
					ConditionExpression:       expr.Condition(),
				},
			})
		}
	}
	builder.items = append(builder.items, items...)
	return builder
}
func (builder *Builder) Error() error {
	return builder.err
}
func (builder *Builder) Run(ctx context.Context, cli Client) (out *dynamodb.TransactWriteItemsOutput, err error) {
	if builder.HasError() {
		err = builder.err
		return
	}
	for i := 0; i < len(builder.items); i += MaxItems {
		end := i + MaxItems
		if end > len(builder.items) {
			end = len(builder.items)
		}
		if out, err = run(ctx, cli, builder.items[i:end]); err != nil {
			return nil, err
		}
	}
	return
}

func run(ctx context.Context, cli Client, items []types.TransactWriteItem) (out *dynamodb.TransactWriteItemsOutput, err error) {
	if len(items) > MaxItems {
		return nil, errors.New("transaction size is within 25 items")
	}
	if out, err = cli.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items}); err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	return
}

type Client interface {
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}
