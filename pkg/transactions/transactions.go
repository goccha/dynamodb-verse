package transactions

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/dynamodb-verse/pkg/foundations"
	"github.com/goccha/dynamodb-verse/pkg/foundations/options"
	"github.com/pkg/errors"
)

const (
	MaxItems = 25
)

type Transaction interface {
	PutItem(ctx context.Context, expiredAt ...time.Time) foundations.WriteItemFunc
	DeleteItem(ctx context.Context) foundations.WriteItemFunc
	UpdateItem(ctx context.Context, fields ...foundations.UpdateField) foundations.WriteItemFunc
}

type transactionItem interface {
	apply(opt ...options.Option) (res types.TransactWriteItem, err error)
}

type putItem struct {
	item *types.Put
}

func (p *putItem) apply(opt ...options.Option) (res types.TransactWriteItem, err error) {
	return types.TransactWriteItem{
		Put: p.item,
	}, nil
}

type delayedPutItem struct {
	key foundations.WriteItemFunc
}

func (p *delayedPutItem) apply(opt ...options.Option) (res types.TransactWriteItem, err error) {
	table, item, expr, err := p.key()
	if err != nil {
		return res, err
	}
	input := &types.Put{
		TableName:                 aws.String(table),
		Item:                      item,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
	}
	for _, f := range opt {
		input = f(input).(*types.Put)
	}
	return types.TransactWriteItem{
		Put: input,
	}, nil
}

type deleteItem struct {
	item *types.Delete
}

func (p *deleteItem) apply(opt ...options.Option) (res types.TransactWriteItem, err error) {
	return types.TransactWriteItem{
		Delete: p.item,
	}, nil
}

type delayedDeleteItem struct {
	key foundations.WriteItemFunc
}

func (p *delayedDeleteItem) apply(opt ...options.Option) (res types.TransactWriteItem, err error) {
	table, item, expr, err := p.key()
	if err != nil {
		return res, err
	}
	input := &types.Delete{
		TableName:                 aws.String(table),
		Key:                       item,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
	}
	for _, f := range opt {
		input = f(input).(*types.Delete)
	}
	return types.TransactWriteItem{
		Delete: input,
	}, nil
}

type updateItem struct {
	item *types.Update
}

func (p *updateItem) apply(opt ...options.Option) (res types.TransactWriteItem, err error) {
	return types.TransactWriteItem{
		Update: p.item,
	}, nil
}

type delayedUpdateItem struct {
	key foundations.WriteItemFunc
}

func (p *delayedUpdateItem) apply(opt ...options.Option) (res types.TransactWriteItem, err error) {
	table, item, expr, err := p.key()
	if err != nil {
		return res, err
	}
	input := &types.Update{
		Key:                       item,
		TableName:                 aws.String(table),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		ConditionExpression:       expr.Condition(),
	}
	for _, f := range opt {
		input = f(input).(*types.Update)
	}
	return types.TransactWriteItem{
		Update: input,
	}, nil
}

func New(opt ...options.Option) *Builder {
	return &Builder{
		items: make([]transactionItem, 0, MaxItems),
		opt:   opt,
	}
}

type Builder struct {
	items []transactionItem
	opt   []options.Option
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
		builder.items = make([]transactionItem, 0, MaxItems)
	}
	for _, k := range keys {
		if table, item, expr, err := k(); err != nil {
			builder.err = err
			return builder
		} else {
			input := &types.Put{
				TableName:                 aws.String(table),
				Item:                      item,
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
				ConditionExpression:       expr.Condition(),
			}
			for _, f := range builder.opt {
				input = f(input).(*types.Put)
			}
			builder.items = append(builder.items, &putItem{item: input})
		}
	}
	return builder
}

func (builder *Builder) DelayedPut(keys ...foundations.WriteItemFunc) *Builder {
	if builder.err != nil {
		return builder
	}
	if builder.items == nil {
		builder.items = make([]transactionItem, 0, MaxItems)
	}
	for _, k := range keys {
		builder.items = append(builder.items, &delayedPutItem{key: k})
	}
	return builder
}

// Delete 削除用
func (builder *Builder) Delete(keys ...foundations.WriteItemFunc) *Builder {
	if builder.err != nil {
		return builder
	}
	if builder.items == nil {
		builder.items = make([]transactionItem, 0, MaxItems)
	}
	for _, k := range keys {
		if table, item, expr, err := k(); err != nil {
			builder.err = err
			return builder
		} else {
			input := &types.Delete{
				TableName:                 aws.String(table),
				Key:                       item,
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
				ConditionExpression:       expr.Condition(),
			}
			for _, f := range builder.opt {
				input = f(input).(*types.Delete)
			}
			builder.items = append(builder.items, &deleteItem{item: input})
		}

	}
	return builder
}

func (builder *Builder) DelayedDelete(keys ...foundations.WriteItemFunc) *Builder {
	if builder.err != nil {
		return builder
	}
	if builder.items == nil {
		builder.items = make([]transactionItem, 0, MaxItems)
	}
	for _, k := range keys {
		builder.items = append(builder.items, &delayedDeleteItem{key: k})
	}
	return builder
}

// Update 更新用
func (builder *Builder) Update(keys ...foundations.WriteItemFunc) *Builder {
	if builder.err != nil {
		return builder
	}
	if builder.items == nil {
		builder.items = make([]transactionItem, 0, MaxItems)
	}
	for _, k := range keys {
		if table, item, expr, err := k(); err != nil {
			builder.err = err
			return builder
		} else {
			input := &types.Update{
				Key:                       item,
				TableName:                 aws.String(table),
				UpdateExpression:          expr.Update(),
				ExpressionAttributeValues: expr.Values(),
				ExpressionAttributeNames:  expr.Names(),
				ConditionExpression:       expr.Condition(),
			}
			for _, f := range builder.opt {
				input = f(input).(*types.Update)
			}
			builder.items = append(builder.items, &updateItem{item: input})
		}
	}
	return builder
}

func (builder *Builder) DelayedUpdate(keys ...foundations.WriteItemFunc) *Builder {
	if builder.err != nil {
		return builder
	}
	if builder.items == nil {
		builder.items = make([]transactionItem, 0, MaxItems)
	}
	for _, k := range keys {
		builder.items = append(builder.items, &delayedUpdateItem{key: k})
	}
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
		if out, err = run(ctx, cli, builder.items[i:end], builder.opt); err != nil {
			return nil, err
		}
	}
	return
}

func run(ctx context.Context, cli Client, items []transactionItem, opt []options.Option) (out *dynamodb.TransactWriteItemsOutput, err error) {
	if len(items) > MaxItems {
		return nil, fmt.Errorf("transaction size is within %d items", MaxItems)
	}
	applies := make([]types.TransactWriteItem, 0, len(items))
	for _, v := range items {
		var item types.TransactWriteItem
		if item, err = v.apply(opt...); err != nil {
			return nil, err
		}
		applies = append(applies, item)
	}
	if out, err = cli.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: applies}); err != nil {
		return nil, errors.WithStack(err)
	}
	return
}

type Client interface {
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	TransactGetItems(ctx context.Context, params *dynamodb.TransactGetItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
}
