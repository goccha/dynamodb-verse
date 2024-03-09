package migrate

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type SchemaBuilder struct {
	schema *TableSchema
}

func NewSchema(name string) *SchemaBuilder {
	return &SchemaBuilder{
		schema: &TableSchema{
			TableName:   name,
			BillingMode: types.BillingModePayPerRequest,
		},
	}
}

func (b *SchemaBuilder) Attributes(attrs ...Attribute) *SchemaBuilder {
	b.schema.Attributes = attrs
	return b
}
func (b *SchemaBuilder) Keys(keys ...KeySchema) *SchemaBuilder {
	b.schema.Keys = keys
	return b
}

func (b *SchemaBuilder) Throughput(read, write int64) *SchemaBuilder {
	b.schema.Throughput = ProvisionedThroughput{Read: read, Write: write}
	return b
}

func (b *SchemaBuilder) BillingMode(mode types.BillingMode) *SchemaBuilder {
	b.schema.BillingMode = mode
	return b
}

func (b *SchemaBuilder) GlobalSecondaryIndex(indexes ...SecondaryIndex) *SchemaBuilder {
	b.schema.GlobalSecondaryIndex = indexes
	return b
}

func (b *SchemaBuilder) LocalSecondaryIndex(indexes ...SecondaryIndex) *SchemaBuilder {
	b.schema.LocalSecondaryIndex = indexes
	return b
}

func (b *SchemaBuilder) TableClass(class types.TableClass) *SchemaBuilder {
	b.schema.TableClass = class
	return b
}

func (b *SchemaBuilder) TimeToLive(attr string, enabled bool) *SchemaBuilder {
	b.schema.TimeToLive = &TimeToLiveSpecification{AttributeName: attr, Enabled: enabled}
	return b
}

func (b *SchemaBuilder) Build(ctx context.Context, api MigrationApi) (*dynamodb.CreateTableOutput, error) {
	if out, err := b.schema.Exists(ctx, api); err != nil {
		return nil, err
	} else if out != nil {
		return nil, errors.New("table already exists")
	}
	return b.schema.Create(ctx, api)
}
