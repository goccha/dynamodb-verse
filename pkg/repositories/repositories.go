package repositories

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/dynamodb-verse/pkg/transactions"
	"github.com/goccha/logging/log"
	"github.com/pkg/errors"
	"reflect"
)

type GetClient interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
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

var ErrNotFound *types.ResourceNotFoundException

func IsNotFound(err error) bool {
	return errors.As(err, &ErrNotFound)
}

type NewGetKey func() (table string, keys map[string]types.AttributeValue, attrs []string, err error)

type FetchItem func(tableName string, value map[string]types.AttributeValue) error

func Get(ctx context.Context, cli GetClient, getKeys NewGetKey, fetch FetchItem) error {
	table, keys, attrs, err := getKeys()
	if err != nil {
		return err
	}
	var out *dynamodb.GetItemOutput
	if out, err = cli.GetItem(ctx, &dynamodb.GetItemInput{
		Key:             keys,
		AttributesToGet: attrs,
		TableName:       aws.String(table),
	}); err != nil {
		if IsNotFound(err) {
			return nil
		}
		return errors.WithStack(err)
	} else if out.Item != nil {
		if err = fetch(table, out.Item); err != nil {
			return errors.WithStack(err)
		} else {
			return nil
		}
	}
	return nil
}

func Put(ctx context.Context, cli WriteClient, items transactions.NewTransactionItem) error {
	table, item, expr, err := items()
	if err != nil {
		return err
	} else {
		var out *dynamodb.PutItemOutput
		if out, err = cli.PutItem(ctx, &dynamodb.PutItemInput{
			Item:                      item,
			TableName:                 aws.String(table),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ConditionExpression:       expr.Condition(),
		}); err != nil {
			return errors.WithStack(err)
		} else {
			log.Debug(ctx).Msgf("%+v", out)
		}
	}
	return nil
}

func Update(ctx context.Context, cli WriteClient, items transactions.NewTransactionItem) error {
	table, item, expr, err := items()
	if err != nil {
		return err
	}
	if _, err = cli.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		Key:                       item,
		TableName:                 aws.String(table),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		ConditionExpression:       expr.Condition(),
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func Delete(ctx context.Context, cli WriteClient, items transactions.NewTransactionItem) error {
	table, keys, expr, err := items()
	if err != nil {
		return err
	}
	if _, err = cli.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		Key:                       keys,
		TableName:                 aws.String(table),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}); err != nil {
		return err
	}
	return nil
}
