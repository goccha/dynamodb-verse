package gen

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gertd/go-pluralize"
	"github.com/goccha/dynamodb-verse/pkg/migrate"
	"github.com/stoewer/go-strcase"
)

type Param struct {
	PackageName      string
	EntityName       string
	TableName        string
	EntitiesName     string
	Fields           []EntityField
	Keys             []EntityKey
	BackQuote        string
	EntityPackage    string
	TablePackage     string
	SecondaryIndexes []SecondaryIndex
}

const BackQuote = "`"

type EntityField struct {
	Name       string
	Type       string
	JsonKey    string
	ColumnName string
	BackQuote  string
}

type EntityKey struct {
	ColumnName string
	Type       string
	FieldName  string
	BackQuote  string
}

type SecondaryIndex struct {
	Name      string
	TableName string
	IndexName string
	Keys      []EntityKey
}

type Output struct {
	Name          string
	entityPackage string
	Entity        string
	tablePackage  string
	Table         string
}

func (out Output) WriteFile(ctx context.Context, path string) error {
	dirPath := filepath.Join(path, out.tablePackage)
	if err := mkdirAll(dirPath); err != nil {
		return err
	}
	tf, err := os.OpenFile(filepath.Join(dirPath, strcase.SnakeCase(out.Name)+".go"), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func() {
		_ = tf.Close()
	}()
	if _, err = io.WriteString(tf, out.Table); err != nil {
		return err
	}
	if out.entityPackage == out.tablePackage {
		if _, err = io.WriteString(tf, out.Entity); err != nil {
			return err
		}
		return nil
	} else {
		dirPath = filepath.Join(path, out.entityPackage)
		if err = mkdirAll(dirPath); err != nil {
			return err
		}
		f, err := os.OpenFile(filepath.Join(dirPath, strcase.SnakeCase(out.Name)+".go"), os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer func() {
			_ = f.Close()
		}()
		if _, err = io.WriteString(f, out.Entity); err != nil {
			return err
		}
	}
	return nil
}

func mkdirAll(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("mkdir %s", path)
			if err = os.MkdirAll(path, 0777); err != nil {
				return err
			}
			if info, err = os.Stat(path); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not directory", path)
	}
	return nil
}

type Outputs []Output

func (out Outputs) WriteFile(ctx context.Context, path string) error {
	for _, v := range out {
		if err := mkdirAll(path); err != nil {
			return err
		}
		if err := v.WriteFile(ctx, path); err != nil {
			return err
		}
	}
	return nil
}

type SchemaSource interface {
	GetSchemas(ctx context.Context) ([]migrate.Schema, error)
}

type FileSource struct {
	Path string
}

func (src FileSource) GetSchemas(ctx context.Context) ([]migrate.Schema, error) {
	return migrate.New(nil, src.Path).Read(ctx)
}

type SchemasSource struct {
	Schemas []migrate.Schema
}

func (src SchemasSource) GetSchemas(ctx context.Context) ([]migrate.Schema, error) {
	return src.Schemas, nil
}

type Options struct {
	PackageName   string
	EntityPackage string
	TablePackage  string
}

func packageName(name string) string {
	packages := strings.Split(name, "/")
	return packages[len(packages)-1]
}

func Generate(ctx context.Context, src SchemaSource, options Options) (Outputs, error) {
	schemas, err := src.GetSchemas(ctx)
	if err != nil {
		return nil, err
	}
	sources := make(Outputs, 0, len(schemas))
	if options.EntityPackage == "" {
		options.EntityPackage = "entities"
	}
	if options.TablePackage == "" {
		options.TablePackage = "repositories/tables"
	}

	plu := pluralize.NewClient()
	for _, schema := range schemas {
		param := Param{
			PackageName:   options.PackageName,
			TableName:     schema.Table.TableName,
			EntityName:    strcase.UpperCamelCase(plu.Singular(schema.Table.TableName)),
			EntitiesName:  strcase.UpperCamelCase(plu.Plural(schema.Table.TableName)),
			BackQuote:     BackQuote,
			EntityPackage: packageName(options.EntityPackage),
			TablePackage:  packageName(options.TablePackage),
		}
		sort.Slice(schema.Table.Keys, func(i, j int) bool {
			return schema.Table.Keys[i].Type < schema.Table.Keys[j].Type
		})
		keyTypes := map[string]string{}
		for _, attr := range schema.Table.Attributes {
			keyTypes[attr.Name] = keyType(attr.Type)
			param.Fields = append(param.Fields, EntityField{
				Name:       strcase.UpperCamelCase(attr.Name),
				Type:       entityType(attr.Type),
				JsonKey:    strcase.SnakeCase(attr.Name),
				ColumnName: strcase.UpperCamelCase(attr.Name),
				BackQuote:  BackQuote,
			})
		}
		for _, key := range schema.Table.Keys {
			param.Keys = append(param.Keys, EntityKey{
				ColumnName: key.Name,
				Type:       keyTypes[key.Name],
				FieldName:  strcase.UpperCamelCase(key.Name),
				BackQuote:  BackQuote,
			})
		}
		for _, index := range schema.Table.GlobalSecondaryIndex {
			sIndex := SecondaryIndex{
				Name:      strcase.UpperCamelCase(index.Name),
				TableName: schema.Table.TableName,
				IndexName: index.Name,
				Keys:      make([]EntityKey, 0, len(index.Keys)),
			}
			for _, key := range index.Keys {
				sIndex.Keys = append(sIndex.Keys, EntityKey{
					ColumnName: key.Name,
					Type:       keyTypes[key.Name],
					FieldName:  strcase.UpperCamelCase(key.Name),
					BackQuote:  BackQuote,
				})
			}
			param.SecondaryIndexes = append(param.SecondaryIndexes, sIndex)
		}
		for _, index := range schema.Table.LocalSecondaryIndex {
			sIndex := SecondaryIndex{
				Name:      strcase.UpperCamelCase(index.Name),
				TableName: schema.Table.TableName,
				IndexName: index.Name,
				Keys:      make([]EntityKey, 0, len(index.Keys)),
			}
			for _, key := range index.Keys {
				sIndex.Keys = append(sIndex.Keys, EntityKey{
					ColumnName: key.Name,
					Type:       keyTypes[key.Name],
					FieldName:  strcase.UpperCamelCase(key.Name),
					BackQuote:  BackQuote,
				})
			}
			param.SecondaryIndexes = append(param.SecondaryIndexes, sIndex)
		}

		output := Output{
			Name:          param.TableName,
			entityPackage: options.EntityPackage,
			tablePackage:  options.TablePackage,
		}
		table, err := template.New("table").Parse(TableTemplate)
		if err != nil {
			return nil, err
		}
		w := strings.Builder{}
		if err = table.Execute(&w, param); err != nil {
			return nil, err
		}
		output.Table = w.String()
		w.Reset()
		entity, err := template.New("entity").Parse(EntityTemplate)
		if err != nil {
			return nil, err
		}
		if err = entity.Execute(&w, param); err != nil {
			return nil, err
		}
		output.Entity = w.String()
		sources = append(sources, output)
	}
	return sources, nil
}

func keyType(attributeType types.ScalarAttributeType) string {
	switch attributeType {
	case types.ScalarAttributeTypeS:
		return "types.AttributeValueMemberS"
	case types.ScalarAttributeTypeN:
		return "types.AttributeValueMemberN"
	case types.ScalarAttributeTypeB:
		return "types.AttributeValueMemberB"
	}
	return ""
}

func entityType(attributeType types.ScalarAttributeType) string {
	switch attributeType {
	case types.ScalarAttributeTypeS:
		return "string"
	case types.ScalarAttributeTypeN:
		return "int"
	case types.ScalarAttributeTypeB:
		return "[]byte"
	}
	return ""
}
