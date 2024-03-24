package migrate

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Attributes []Attribute

func (attrs Attributes) Map() map[string]types.ScalarAttributeType {
	set := make(map[string]types.ScalarAttributeType)
	for _, a := range attrs {
		set[a.Name] = a.Type
	}
	return set
}
func (attrs Attributes) Definitions() []types.AttributeDefinition {
	array := make([]types.AttributeDefinition, 0, len(attrs))
	for _, a := range attrs {
		array = append(array, a.Definition())
	}
	return array
}

type Attribute struct {
	Type types.ScalarAttributeType `json:"AttributeType" yaml:"AttributeType"`
	Name string                    `json:"AttributeName" yaml:"AttributeName"`
}

func (attr Attribute) Definition() types.AttributeDefinition {
	return types.AttributeDefinition{
		AttributeName: aws.String(attr.Name),
		AttributeType: attr.Type,
	}
}

func (attr Attribute) HashKey() KeySchema {
	return KeySchema{Name: attr.Name, Type: types.KeyTypeHash}
}

func (attr Attribute) RangeKey() KeySchema {
	return KeySchema{Name: attr.Name, Type: types.KeyTypeRange}
}

type Keys []KeySchema

func (keys Keys) Map() map[string]types.KeyType {
	m := make(map[string]types.KeyType)
	for _, k := range keys {
		m[k.Name] = k.Type
	}
	return m
}
func (keys Keys) Elements() []types.KeySchemaElement {
	array := make([]types.KeySchemaElement, 0, len(keys))
	for _, k := range keys {
		array = append(array, *k.Element())
	}
	return array
}

type KeySchema struct {
	Type types.KeyType `json:"KeyType" yaml:"KeyType"`
	Name string        `json:"AttributeName" yaml:"AttributeName"`
}

func (k KeySchema) Element() *types.KeySchemaElement {
	return &types.KeySchemaElement{
		AttributeName: aws.String(k.Name),
		KeyType:       k.Type,
	}
}

type ProvisionedThroughput struct {
	Read  int64 `json:"ReadCapacityUnits" yaml:"ReadCapacityUnits"`
	Write int64 `json:"WriteCapacityUnits" yaml:"WriteCapacityUnits"`
}

func (t ProvisionedThroughput) Element() *types.ProvisionedThroughput {
	if t.Read > 0 {
		return &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(t.Read),
			WriteCapacityUnits: aws.Int64(t.Write),
		}
	}
	return nil
}
func (t ProvisionedThroughput) Update(desc types.TableDescription) *types.ProvisionedThroughput {
	if desc.ProvisionedThroughput != nil {
		if *desc.ProvisionedThroughput.ReadCapacityUnits != t.Read ||
			*desc.ProvisionedThroughput.WriteCapacityUnits != t.Write {
			return &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(t.Read),
				WriteCapacityUnits: aws.Int64(t.Write),
			}
		}
	} else if t.Read > 0 || t.Write > 0 {
		return &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(t.Read),
			WriteCapacityUnits: aws.Int64(t.Write),
		}
	}
	return nil
}

type Projection struct {
	Type           types.ProjectionType `json:"ProjectionType" yaml:"ProjectionType"`
	AttributeNames []string             `json:"NonKeyAttributes" yaml:"NonKeyAttributes"`
}

func (p Projection) Element() *types.Projection {
	return &types.Projection{
		NonKeyAttributes: p.AttributeNames,
		ProjectionType:   p.Type,
	}
}

type TimeToLiveSpecification struct {
	AttributeName string `json:"AttributeName" yaml:"AttributeName"`
	Enabled       bool   `json:"Enabled" yaml:"Enabled"`
}

func (s TimeToLiveSpecification) Element() *types.TimeToLiveSpecification {
	return &types.TimeToLiveSpecification{
		AttributeName: aws.String(s.AttributeName),
		Enabled:       aws.Bool(s.Enabled),
	}
}
