package foundations

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

type GetClient interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

type QueryClient interface {
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

type WriteClient interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

func IsNil(record interface{}) bool {
	if (record == nil) || reflect.ValueOf(record).IsNil() {
		return true
	} else {
		return false
	}
}

type RecordNotFoundException struct{}

func (err *RecordNotFoundException) Error() string {
	return "record not found"
}

var ErrNotFound *RecordNotFoundException

func IsNotFound(err error) bool {
	return errors.As(err, &ErrNotFound)
}

type GetKeyFunc func() (table string, keys map[string]types.AttributeValue, attrs []string, err error)

type QueryConditionFunc func() (table, index string, expr expression.Expression, desc bool, err error)

type FetchItemFunc func(tableName string, value map[string]types.AttributeValue) error

type FetchItemsFunc func(tableName string, value []map[string]types.AttributeValue) error

func Get(ctx context.Context, cli GetClient, getKeys GetKeyFunc, fetch FetchItemFunc) error {
	table, keys, attrs, err := getKeys()
	if err != nil {
		return err
	}
	var out *dynamodb.GetItemOutput
	if out, err = cli.GetItem(ctx, &dynamodb.GetItemInput{
		Key:             keys,
		AttributesToGet: attrs,
		TableName:       &table,
	}); err != nil {
		return errors.WithStack(err)
	} else if out.Item != nil {
		if err = fetch(table, out.Item); err != nil {
			return errors.WithStack(err)
		} else {
			return nil
		}
	}
	return ErrNotFound
}

func Query(ctx context.Context, cli QueryClient, condition QueryConditionFunc, fetch FetchItemsFunc, limit ...int32) error {
	table, index, expr, desc, err := condition()
	if err != nil {
		return err
	}
	var out *dynamodb.QueryOutput
	var indexName *string
	if index != "" {
		indexName = aws.String(index)
	}
	var queryLimit *int32
	if len(limit) > 0 {
		queryLimit = &limit[0]
	}
	if out, err = cli.Query(ctx, &dynamodb.QueryInput{
		TableName:                 &table,
		IndexName:                 indexName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		Limit:                     queryLimit,
		ScanIndexForward:          aws.Bool(!desc),
	}); err != nil {
		return errors.WithStack(err)
	} else if len(out.Items) > 0 {
		if err = fetch(table, out.Items); err != nil {
			return err
		}
	}
	return ErrNotFound
}

func Put(ctx context.Context, cli WriteClient, items WriteItemFunc) error {
	table, item, expr, err := items()
	if err != nil {
		return err
	} else {
		if _, err = cli.PutItem(ctx, &dynamodb.PutItemInput{
			Item:                      item,
			TableName:                 &table,
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ConditionExpression:       expr.Condition(),
		}); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func Update(ctx context.Context, cli WriteClient, items WriteItemFunc) error {
	table, item, expr, err := items()
	if err != nil {
		return err
	}
	if _, err = cli.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		Key:                       item,
		TableName:                 &table,
		UpdateExpression:          expr.Update(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		ConditionExpression:       expr.Condition(),
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func Delete(ctx context.Context, cli WriteClient, items WriteItemFunc) error {
	table, keys, expr, err := items()
	if err != nil {
		return err
	}
	if _, err = cli.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		Key:                       keys,
		TableName:                 &table,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}); err != nil {
		return err
	}
	return nil
}

type WriteItemFunc func() (table string, item map[string]types.AttributeValue, expr expression.Expression, err error)

type UpdateField func(ctx context.Context, builder *expression.UpdateBuilder) expression.UpdateBuilder

func UpdateItems(ctx context.Context, fields ...UpdateField) expression.UpdateBuilder {
	var builder expression.UpdateBuilder
	for _, v := range fields {
		builder = v(ctx, &builder)
	}
	return builder
}
