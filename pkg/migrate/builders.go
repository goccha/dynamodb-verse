package migrate

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func NewStringAttribute(name string) Attribute {
	return Attribute{Name: name, Type: types.ScalarAttributeTypeS}
}

func NewNumberAttribute(name string) Attribute {
	return Attribute{Name: name, Type: types.ScalarAttributeTypeN}
}

func NewBinaryAttribute(name string) Attribute {
	return Attribute{Name: name, Type: types.ScalarAttributeTypeB}
}

func NewHashKey(name string) KeySchema {
	return KeySchema{Name: name, Type: types.KeyTypeHash}
}

func NewRangeKey(name string) KeySchema {
	return KeySchema{Name: name, Type: types.KeyTypeRange}
}

func NewProjection(t types.ProjectionType, attrs ...string) *Projection {
	return &Projection{Type: t, AttributeNames: attrs}
}

type IndexOption func(si *SecondaryIndex)

func WithIndexThroughput(read, write int64) IndexOption {
	return func(si *SecondaryIndex) {
		si.Throughput = &ProvisionedThroughput{Read: read, Write: write}
	}
}

func WithIndexProjection(t types.ProjectionType, attrs ...string) IndexOption {
	return func(si *SecondaryIndex) {
		si.Projection = NewProjection(t, attrs...)
	}
}

func NewKeys(hashKey KeySchema, rangeKey ...KeySchema) Keys {
	if len(rangeKey) == 0 {
		return Keys{hashKey}
	}
	return Keys{hashKey, rangeKey[0]}
}

func NewSecondaryIndex(name string, keys Keys, opts ...IndexOption) SecondaryIndex {
	si := &SecondaryIndex{Name: name, Keys: keys,
		Projection: &Projection{
			Type: types.ProjectionTypeAll,
		},
	}
	for _, opt := range opts {
		opt(si)
	}
	return *si
}

type SchemaBuilder struct {
	schema *Schema
}

func NewSchema(name string) *SchemaBuilder {
	return &SchemaBuilder{
		schema: &Schema{
			name: name,
			Table: TableSchema{
				TableName:   name,
				BillingMode: types.BillingModePayPerRequest,
			},
		},
	}
}

func (b *SchemaBuilder) Attributes(attrs ...Attribute) *SchemaBuilder {
	b.schema.Table.Attributes = attrs
	return b
}
func (b *SchemaBuilder) Keys(keys ...KeySchema) *SchemaBuilder {
	b.schema.Table.Keys = keys
	return b
}

func (b *SchemaBuilder) Throughput(read, write int64) *SchemaBuilder {
	b.schema.Table.Throughput = ProvisionedThroughput{Read: read, Write: write}
	return b
}

func (b *SchemaBuilder) BillingMode(mode types.BillingMode) *SchemaBuilder {
	b.schema.Table.BillingMode = mode
	return b
}

func (b *SchemaBuilder) GlobalSecondaryIndex(indexes ...SecondaryIndex) *SchemaBuilder {
	b.schema.Table.GlobalSecondaryIndex = indexes
	return b
}

func (b *SchemaBuilder) LocalSecondaryIndex(indexes ...SecondaryIndex) *SchemaBuilder {
	b.schema.Table.LocalSecondaryIndex = indexes
	return b
}

func (b *SchemaBuilder) TableClass(class types.TableClass) *SchemaBuilder {
	b.schema.Table.TableClass = class
	return b
}

func (b *SchemaBuilder) TimeToLive(attr string, enabled bool) *SchemaBuilder {
	b.schema.Table.TimeToLive = &TimeToLiveSpecification{AttributeName: attr, Enabled: enabled}
	return b
}

var ErrTableAlreadyExists = errors.New("table already exists")

func (b *SchemaBuilder) Build(ctx context.Context, api MigrationApi, opt ...TableSchemaOption) (*dynamodb.CreateTableOutput, error) {
	for _, o := range opt {
		o(&b.schema.Table)
	}
	if out, err := b.schema.Table.Exists(ctx, api); err != nil {
		return nil, err
	} else if out != nil {
		return nil, ErrTableAlreadyExists
	}
	return b.schema.Table.Create(ctx, api)
}

func (b *SchemaBuilder) Get() Schema {
	return *b.schema
}

func GetSchemas(builders ...*SchemaBuilder) Schemas {
	schemas := make([]Schema, 0, len(builders))
	for _, b := range builders {
		schemas = append(schemas, b.Get())
	}
	return schemas
}

type Schemas []Schema

func (src Schemas) GetSchemas(ctx context.Context) ([]Schema, error) {
	return src, nil
}

type TableSchemaOption func(ts *TableSchema)

func WithTableNamePrefix(prefix string) TableSchemaOption {
	return func(ts *TableSchema) {
		ts.tableNamePrefix = prefix
	}
}

type SchemaBuilders []*SchemaBuilder

func (b SchemaBuilders) Build(ctx context.Context, api MigrationApi, opt ...TableSchemaOption) ([]*dynamodb.CreateTableOutput, error) {
	outputs := make([]*dynamodb.CreateTableOutput, 0, len(b))
	for _, builder := range b {
		if out, err := builder.Build(ctx, api, opt...); err != nil {
			if !errors.Is(err, ErrTableAlreadyExists) {
				return nil, err
			}
		} else {
			outputs = append(outputs, out)
		}
	}
	return outputs, nil
}
