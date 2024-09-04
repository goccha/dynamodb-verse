package migrate

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

type TableSchema struct {
	TableName            string                   `json:"TableName" yaml:"TableName"`
	Attributes           Attributes               `json:"AttributeDefinitions" yaml:"AttributeDefinitions"`
	Keys                 Keys                     `json:"KeySchema" yaml:"KeySchema"`
	Throughput           ProvisionedThroughput    `json:"ProvisionedThroughput" yaml:"ProvisionedThroughput"`
	BillingMode          types.BillingMode        `json:"BillingMode" yaml:"BillingMode"`
	GlobalSecondaryIndex SecondaryIndexes         `json:"GlobalSecondaryIndexes,omitempty" yaml:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndex  SecondaryIndexes         `json:"LocalSecondaryIndexes,omitempty" yaml:"LocalSecondaryIndexes,omitempty"`
	TableClass           types.TableClass         `json:"TableClass" yaml:"TableClass"`
	TimeToLive           *TimeToLiveSpecification `json:"TimeToLiveSpecification,omitempty" yaml:"TimeToLiveSpecification,omitempty"`
	tableNamePrefix      string
}

func (t TableSchema) billingMode() types.BillingMode {
	if t.BillingMode == "" {
		if t.Throughput.Read > 0 {
			t.BillingMode = types.BillingModeProvisioned
		} else {
			t.BillingMode = types.BillingModePayPerRequest
		}
	}
	return t.BillingMode
}

func (t TableSchema) Exists(ctx context.Context, api MigrationApi) (out *dynamodb.DescribeTableOutput, err error) {
	if out, err = api.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(t.tableNamePrefix + t.TableName)}); err != nil {
		if IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.WithStack(err) // テーブルが存在しない以外のエラー
	}
	return out, nil
}

func (t TableSchema) Create(ctx context.Context, api MigrationApi) (out *dynamodb.CreateTableOutput, err error) {
	attrs := t.Attributes.Definitions()
	keys := t.Keys.Elements()
	tp := t.Throughput.Element()
	var g []types.GlobalSecondaryIndex
	if t.GlobalSecondaryIndex != nil && len(t.GlobalSecondaryIndex) > 0 {
		g = t.GlobalSecondaryIndex.GlobalIndexes()
	}
	var l []types.LocalSecondaryIndex
	if t.LocalSecondaryIndex != nil && len(t.LocalSecondaryIndex) > 0 {
		l = t.LocalSecondaryIndex.LocalIndexes()
	}
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions:   attrs,
		KeySchema:              keys,
		TableName:              aws.String(t.tableNamePrefix + t.TableName),
		BillingMode:            t.billingMode(),
		ProvisionedThroughput:  tp,
		GlobalSecondaryIndexes: g,
		LocalSecondaryIndexes:  l,
		SSESpecification:       nil, // TODO
		StreamSpecification:    nil, // TODO
		TableClass:             t.TableClass,
		Tags:                   nil, // TODO
	}
	if out, err = api.CreateTable(ctx, input); err != nil {
		return nil, errors.WithStack(err)
	}
	if t.TimeToLive != nil {
		if _, err = api.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
			TableName:               aws.String(t.tableNamePrefix + t.TableName),
			TimeToLiveSpecification: t.TimeToLive.Element(),
		}); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return
}

func (t TableSchema) Update(ctx context.Context, api MigrationApi, desc types.TableDescription) (out *dynamodb.UpdateTableOutput, err error) {
	in := &dynamodb.UpdateTableInput{
		TableName:                   aws.String(t.tableNamePrefix + t.TableName),
		AttributeDefinitions:        t.Attributes.Definitions(),
		GlobalSecondaryIndexUpdates: t.GlobalSecondaryIndex.UpdateGlobals(desc),
		ProvisionedThroughput:       t.Throughput.Update(desc),
		ReplicaUpdates:              nil, // TODO
		SSESpecification:            nil, // TODO
		StreamSpecification:         nil, // TODO
	}
	if t.BillingMode != desc.BillingModeSummary.BillingMode {
		in.BillingMode = t.BillingMode
	}
	if t.TableClass != "" && t.TableClass != desc.TableClassSummary.TableClass {
		in.TableClass = t.TableClass
	}
	if out, err = api.UpdateTable(ctx, in); err != nil {
		return nil, errors.WithStack(err)
	}
	if t.TimeToLive != nil {
		var ttl *dynamodb.DescribeTimeToLiveOutput
		if ttl, err = api.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(t.tableNamePrefix + t.TableName)}); err != nil {
			return nil, errors.WithStack(err)
		} else {
			update := false
			switch ttl.TimeToLiveDescription.TimeToLiveStatus {
			case types.TimeToLiveStatusEnabled, types.TimeToLiveStatusEnabling:
				update = !t.TimeToLive.Enabled
			case types.TimeToLiveStatusDisabled, types.TimeToLiveStatusDisabling:
				update = t.TimeToLive.Enabled
			}
			if update {
				if _, err = api.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
					TableName:               aws.String(t.tableNamePrefix + t.TableName),
					TimeToLiveSpecification: t.TimeToLive.Element(),
				}); err != nil {
					return nil, errors.WithStack(err)
				}
			}
		}
	}
	return
}

func (t TableSchema) Delete(ctx context.Context, api MigrationApi) (out *dynamodb.DeleteTableOutput, err error) {
	if out, err = api.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(t.tableNamePrefix + t.TableName)}); err != nil {
		return nil, errors.WithStack(err)
	}
	return
}
