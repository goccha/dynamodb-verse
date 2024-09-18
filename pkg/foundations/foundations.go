package foundations

import (
	"context"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/dynamodb-verse/pkg/foundations/options"
	"github.com/pkg/errors"
)

type Client interface {
	GetClient
	ScanClient
	QueryClient
	WriteClient
}

type GetClient interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

type ScanClient interface {
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
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

var ErrNotFound *types.ResourceNotFoundException

func IsNotFound(err error) bool {
	var notFound *types.ResourceNotFoundException
	return errors.As(err, &notFound)
}

func NotFound(tableName string) *types.ResourceNotFoundException {
	msg := fmt.Sprintf("Requested resource not found: %s: record not found", tableName)
	return &types.ResourceNotFoundException{Message: &msg}
}

type GetKeyFunc func() (table string, keys map[string]types.AttributeValue, attrs []string, err error)

type QueryConditionFunc func() (table, index string, expr expression.Expression, err error)

type ScanFilterFunc func() (table string, expr expression.Expression, err error)

type FetchItemFunc func(tableName string, value Record) error

type FetchItemsFunc func(tableName string, value Records) error

func Get(ctx context.Context, cli GetClient, getKeys GetKeyFunc, fetch FetchItemFunc, opt ...options.Option) (*dynamodb.GetItemOutput, error) {
	table, keys, attrs, err := getKeys()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.GetItemInput{
		Key:             keys,
		AttributesToGet: attrs,
		TableName:       &table,
	}
	if len(opt) > 0 {
		for _, f := range opt {
			input = f(input).(*dynamodb.GetItemInput)
		}
	}
	var out *dynamodb.GetItemOutput
	if out, err = cli.GetItem(ctx, input); err != nil {
		return nil, errors.WithStack(err)
	} else if out.Item != nil {
		if err = fetch(table, out.Item); err != nil {
			return nil, errors.WithStack(err)
		} else {
			return out, nil
		}
	}
	return nil, errors.WithStack(NotFound(table))
}

// EnableErrorWithEmptyList Make the list return an error if it is empty.
func EnableErrorWithEmptyList(v bool) {
	errorWithEmptyList = v
}

var errorWithEmptyList = false

func Scan(ctx context.Context, cli ScanClient, condition ScanFilterFunc, fetch FetchItemsFunc, opt ...options.Option) (*dynamodb.ScanOutput, error) {
	table, expr, err := condition()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.ScanInput{
		TableName:                 &table,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
	}
	if len(opt) > 0 {
		for _, f := range opt {
			input = f(input).(*dynamodb.ScanInput)
		}
	}
	out, err := cli.Scan(ctx, input)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(out.Items) > 0 {
		if err = fetch(table, out.Items); err != nil {
			return nil, err
		} else {
			return out, nil
		}
	}
	if errorWithEmptyList {
		return nil, errors.WithStack(NotFound(table))
	}
	return out, nil
}

func ScanAll(ctx context.Context, cli ScanClient, condition ScanFilterFunc, fetch FetchItemsFunc, opt ...options.Option) (out *dynamodb.ScanOutput, err error) {
	var key EvaluatedKey
	for {
		opts := make([]options.Option, len(opt))
		copy(opts, opt)
		if key != nil {
			opts = append(opts, options.ExclusiveStartKey(key))
		}
		out, err = Scan(ctx, cli, condition, fetch, opts...)
		if err != nil {
			return nil, err
		}
		if out.LastEvaluatedKey == nil {
			break
		}
		key = out.LastEvaluatedKey
	}
	return out, nil
}

func Query(ctx context.Context, cli QueryClient, condition QueryConditionFunc, fetch FetchItemsFunc, opt ...options.Option) (*dynamodb.QueryOutput, error) {
	table, index, expr, err := condition()
	if err != nil {
		return nil, err
	}
	var out *dynamodb.QueryOutput
	var indexName *string
	if index != "" {
		indexName = aws.String(index)
	}
	input := &dynamodb.QueryInput{
		TableName:                 &table,
		IndexName:                 indexName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
	}
	if len(opt) > 0 {
		for _, f := range opt {
			input = f(input).(*dynamodb.QueryInput)
		}
	}
	if out, err = cli.Query(ctx, input); err != nil {
		return nil, errors.WithStack(err)
	} else if len(out.Items) > 0 {
		if err = fetch(table, out.Items); err != nil {
			return nil, err
		} else {
			return out, nil
		}
	}
	if errorWithEmptyList {
		return nil, errors.WithStack(NotFound(table))
	}
	return out, nil
}

func Put(ctx context.Context, cli WriteClient, items WriteItemFunc, opt ...options.Option) (*dynamodb.PutItemOutput, error) {
	table, item, expr, err := items()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.PutItemInput{
		Item:                      item,
		TableName:                 &table,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
	}
	if len(opt) > 0 {
		for _, f := range opt {
			input = f(input).(*dynamodb.PutItemInput)
		}
	}
	var out *dynamodb.PutItemOutput
	if out, err = cli.PutItem(ctx, input); err != nil {
		return nil, errors.WithStack(err)
	}
	return out, nil
}

func Update(ctx context.Context, cli WriteClient, items WriteItemFunc, opt ...options.Option) (*dynamodb.UpdateItemOutput, error) {
	table, item, expr, err := items()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.UpdateItemInput{
		Key:                       item,
		TableName:                 &table,
		UpdateExpression:          expr.Update(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		ConditionExpression:       expr.Condition(),
	}
	if len(opt) > 0 {
		for _, f := range opt {
			input = f(input).(*dynamodb.UpdateItemInput)
		}
	}
	var out *dynamodb.UpdateItemOutput
	if out, err = cli.UpdateItem(ctx, input); err != nil {
		return nil, errors.WithStack(err)
	}
	return out, nil
}

func Delete(ctx context.Context, cli WriteClient, items WriteItemFunc, opt ...options.Option) (*dynamodb.DeleteItemOutput, error) {
	table, keys, expr, err := items()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.DeleteItemInput{
		Key:                       keys,
		TableName:                 &table,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	if len(opt) > 0 {
		for _, f := range opt {
			input = f(input).(*dynamodb.DeleteItemInput)
		}
	}
	var out *dynamodb.DeleteItemOutput
	if out, err = cli.DeleteItem(ctx, input); err != nil {
		return nil, err
	}
	return out, nil
}

type WriteItemFunc func() (table string, item map[string]types.AttributeValue, expr expression.Expression, err error)
type GetItemFunc WriteItemFunc

type UpdateField func(ctx context.Context, builder *expression.UpdateBuilder) expression.UpdateBuilder

func UpdateBuilder(ctx context.Context, fields ...UpdateField) expression.UpdateBuilder {
	var builder expression.UpdateBuilder
	for _, v := range fields {
		builder = v(ctx, &builder)
	}
	return builder
}

type PutItemPreprocessor interface {
	BeforePutItem(ctx context.Context) (any, error)
}

type FetchItemPostprocessor interface {
	AfterFetchItem(ctx context.Context) error
}

type FetchItemsPostprocessor interface {
	AfterFetchItems(ctx context.Context) error
}

type ExpressionFunc func() (expr expression.Expression, err error)

func PutItem(ctx context.Context, tableName string, rec any, f ...ExpressionFunc) WriteItemFunc {
	return func() (table string, item map[string]types.AttributeValue, expr expression.Expression, err error) {
		if preprocessor, ok := rec.(PutItemPreprocessor); ok {
			rec, err = preprocessor.BeforePutItem(ctx)
			if err != nil {
				err = errors.WithStack(err)
				return
			}
		}
		if item, err = attributevalue.MarshalMap(rec); err != nil {
			err = errors.WithStack(err)
			return
		}
		if len(f) > 0 {
			expr, err = f[0]()
		}
		table = tableName
		return
	}
}

func DeleteItem(keyFunc GetKeyFunc) WriteItemFunc {
	return func() (table string, keys map[string]types.AttributeValue, expr expression.Expression, err error) {
		table, keys, _, err = keyFunc()
		return
	}
}

func UpdateItem(ctx context.Context, keyFunc GetKeyFunc, fields ...UpdateField) WriteItemFunc {
	return func() (table string, item map[string]types.AttributeValue, expr expression.Expression, err error) {
		table, item, _, err = keyFunc()
		if err != nil {
			return
		}
		builder := UpdateBuilder(ctx, fields...)
		if expr, err = expression.NewBuilder().WithUpdate(builder).Build(); err != nil {
			err = errors.WithStack(err)
			return
		}
		return
	}
}

func ConsistentUpdateItem(ctx context.Context, keyFunc GetKeyFunc, fieldName string, count int, fields ...UpdateField) WriteItemFunc {
	return func() (table string, item map[string]types.AttributeValue, expr expression.Expression, err error) {
		table, item, _, err = keyFunc()
		if err != nil {
			return
		}
		builder := UpdateBuilder(ctx, fields...).Set(expression.Name(fieldName), expression.Value(count+1))
		condition := expression.Equal(expression.Name(fieldName), expression.Value(count))
		if expr, err = expression.NewBuilder().WithUpdate(builder).WithCondition(condition).Build(); err != nil {
			err = errors.WithStack(err)
			return
		}
		return
	}
}

func FetchItem[T any](ctx context.Context, rec *T) FetchItemFunc {
	return func(tableName string, value Record) error {
		if err := value.Unmarshal(ctx, rec); err != nil {
			return err
		}
		return nil
	}
}

func FetchItems[T any](ctx context.Context, rec *T) FetchItemsFunc {
	return func(tableName string, values Records) error {
		if err := values.Unmarshal(ctx, rec); err != nil {
			return err
		}
		return nil
	}
}

func FetchAll[T any](ctx context.Context, list []T) FetchItemsFunc {
	return func(tableName string, values Records) error {
		var rec T
		if err := values.Unmarshal(ctx, &rec); err != nil {
			return err
		}
		list = append(list, rec)
		return nil
	}
}

func UpdateValue(name string, value any) UpdateField {
	return SetValue(name, value)
}

func SetValue(name string, value any) UpdateField {
	return func(ctx context.Context, builder *expression.UpdateBuilder) expression.UpdateBuilder {
		if builder == nil {
			return expression.Set(expression.Name(name), expression.Value(value))
		}
		return builder.Set(expression.Name(name), expression.Value(value))
	}
}

func RemoveValue(name string) UpdateField {
	return func(ctx context.Context, builder *expression.UpdateBuilder) expression.UpdateBuilder {
		if builder == nil {
			return expression.Remove(expression.Name(name))
		}
		return builder.Remove(expression.Name(name))
	}
}

func AddValue(name string, value any) UpdateField {
	return func(ctx context.Context, builder *expression.UpdateBuilder) expression.UpdateBuilder {
		if builder == nil {
			return expression.Add(expression.Name(name), expression.Value(value))
		}
		return builder.Add(expression.Name(name), expression.Value(value))
	}
}

func DeleteValue(name string, value any) UpdateField {
	return func(ctx context.Context, builder *expression.UpdateBuilder) expression.UpdateBuilder {
		if builder == nil {
			return expression.Delete(expression.Name(name), expression.Value(value))
		}
		return builder.Delete(expression.Name(name), expression.Value(value))
	}
}

type Record map[string]types.AttributeValue

func (r Record) Unmarshal(ctx context.Context, v any) error {
	if err := attributevalue.UnmarshalMap(r, v); err != nil {
		return errors.WithStack(err)
	}
	if postprocessor, ok := v.(FetchItemPostprocessor); ok {
		err := postprocessor.AfterFetchItem(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

type Records []map[string]types.AttributeValue

func (r Records) Unmarshal(ctx context.Context, v any) error {
	if err := attributevalue.UnmarshalListOfMaps(r, v); err != nil {
		return errors.WithStack(err)
	}
	if postprocessor, ok := v.(FetchItemsPostprocessor); ok {
		err := postprocessor.AfterFetchItems(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
	} /*else {
		items := reflect.ValueOf(v).Elem()
		for i := 0; i < items.Len(); i++ {
			item := items.Index(i).Interface()
			if postprocessor, ok := item.(FetchItemPostprocessor); ok {
				err := postprocessor.AfterFetchItem(ctx)
				if err != nil {
					return errors.WithStack(err)
				}
			} else {
				break
			}
		}
	}*/
	return nil
}

func Values[T any](values ...T) (right expression.OperandBuilder, other []expression.OperandBuilder) {
	other = make([]expression.OperandBuilder, 0, len(values))
	for i, v := range values {
		if i == 0 {
			right = expression.Value(v)
		} else {
			other = append(other, expression.Value(v))
		}
	}
	return right, other
}
