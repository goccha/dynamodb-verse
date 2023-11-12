package transactions

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/dynamodb-verse/pkg/foundations"
	"github.com/pkg/errors"
)

const (
	MaxGetItems = 100
)

type GetBuilder struct {
	items []types.TransactGetItem
	err   error
}

func (builder *GetBuilder) HasError() bool {
	return builder.err != nil
}

func Get(keys ...foundations.GetItemFunc) *GetBuilder {
	items := make([]types.TransactGetItem, 0, MaxGetItems)
	for _, k := range keys {
		table, key, expr, err := k()
		if err != nil {
			return &GetBuilder{
				err: err,
			}
		}
		items = append(items, types.TransactGetItem{
			Get: &types.Get{
				Key:                      key,
				TableName:                &table,
				ExpressionAttributeNames: expr.Names(),
				ProjectionExpression:     expr.Projection(),
			},
		})
	}
	return &GetBuilder{
		items: items,
	}
}

func (builder *GetBuilder) Run(ctx context.Context, cli Client, fetch foundations.FetchItemFunc) (out *dynamodb.TransactGetItemsOutput, err error) {
	if builder.HasError() {
		err = builder.err
		return
	}
	for i := 0; i < len(builder.items); i += MaxGetItems {
		end := i + MaxGetItems
		if end > len(builder.items) {
			end = len(builder.items)
		}
		if out, err = get(ctx, cli, builder.items[i:end], fetch); err != nil {
			return nil, err
		}
	}
	return
}

func get(ctx context.Context, cli Client, items []types.TransactGetItem, fetch foundations.FetchItemFunc) (out *dynamodb.TransactGetItemsOutput, err error) {
	if len(items) > MaxGetItems {
		return nil, fmt.Errorf("transaction size is within %d items", MaxGetItems)
	}
	if out, err = cli.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{TransactItems: items}); err != nil {
		return nil, errors.WithStack(err)
	}
	for i, v := range out.Responses { // each of which corresponds to the TransactGetItem object in the same position in the TransactItems array
		if err = fetch(*items[i].Get.TableName, v.Item); err != nil {
			return out, err
		}
	}
	return
}
