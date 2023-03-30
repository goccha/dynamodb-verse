package options

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Option func(input any) any

func ReturnConsumedCapacity(capacity types.ReturnConsumedCapacity) Option {
	return func(input any) any {
		switch in := input.(type) {
		case *dynamodb.PutItemInput:
			in.ReturnConsumedCapacity = capacity
		case *dynamodb.UpdateItemInput:
			in.ReturnConsumedCapacity = capacity
		case *dynamodb.DeleteItemInput:
			in.ReturnConsumedCapacity = capacity
		case *dynamodb.GetItemInput:
			in.ReturnConsumedCapacity = capacity
		case *dynamodb.QueryInput:
			in.ReturnConsumedCapacity = capacity
		case *dynamodb.ScanInput:
			in.ReturnConsumedCapacity = capacity
		}
		return input
	}
}

func ReturnItemCollectionMetrics(metrics types.ReturnItemCollectionMetrics) Option {
	return func(input any) any {
		switch in := input.(type) {
		case *dynamodb.PutItemInput:
			in.ReturnItemCollectionMetrics = metrics
		case *dynamodb.UpdateItemInput:
			in.ReturnItemCollectionMetrics = metrics
		case *dynamodb.DeleteItemInput:
			in.ReturnItemCollectionMetrics = metrics
		}
		return input
	}
}

func ReturnValues(value types.ReturnValue) Option {
	return func(input any) any {
		switch in := input.(type) {
		case *dynamodb.PutItemInput:
			in.ReturnValues = value
		case *dynamodb.UpdateItemInput:
			in.ReturnValues = value
		case *dynamodb.DeleteItemInput:
			in.ReturnValues = value
		}
		return input
	}
}

// AttributeUpdates dynamodb.UpdateItemInput.AttributeUpdates
func AttributeUpdates(updates map[string]types.AttributeValueUpdate) Option {
	return func(input any) any {
		if in, ok := input.(*dynamodb.UpdateItemInput); ok {
			in.AttributeUpdates = updates
		}
		return input
	}
}

// Limit dynamodb.QueryInput.Limit
func Limit(limit int32) Option {
	return func(input any) any {
		if in, ok := input.(*dynamodb.QueryInput); ok {
			in.Limit = &limit
		}
		return input
	}
}

// ScanIndexForward dynamodb.QueryInput.ScanIndexForward
func ScanIndexForward(asc bool) Option {
	return func(input any) any {
		if in, ok := input.(*dynamodb.QueryInput); ok {
			in.ScanIndexForward = &asc
		}
		return input
	}
}
