package migrate

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/logging/log"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type MigrationApi interface {
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	UpdateTimeToLive(ctx context.Context, params *dynamodb.UpdateTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error)
	UpdateTable(ctx context.Context, params *dynamodb.UpdateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
	DescribeTimeToLive(ctx context.Context, params *dynamodb.DescribeTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error)
	DeleteTable(ctx context.Context, params *dynamodb.DeleteTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)

	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

type Migrate interface {
	Read(ctx context.Context) (schemas []Schema, err error)
	Run(ctx context.Context, save SaveFunc) error
}

func Parse(api MigrationApi, schema string) Migrate {
	return &SchemaMigrate{
		api:    api,
		schema: schema,
	}
}

type SchemaMigrate struct {
	api    MigrationApi
	schema string
}

func (v *SchemaMigrate) Read(ctx context.Context) (schemas []Schema, err error) {
	ts := &TableSchema{}
	if err = yaml.Unmarshal([]byte(v.schema), ts); err != nil {
		return nil, err
	}
	return []Schema{{Table: *ts}}, nil
}

func (v *SchemaMigrate) Run(ctx context.Context, save SaveFunc) (err error) {
	ts := &TableSchema{}
	if err = yaml.Unmarshal([]byte(v.schema), ts); err != nil {
		return err
	}
	if _, err = ts.Exists(ctx, v.api); err != nil {
		return err
	}
	_, err = ts.Create(ctx, v.api)
	return err
}

type FilesMigrate struct {
	api     MigrationApi
	dirPath []string
}

func New(api MigrationApi, dirPath ...string) Migrate {
	return &FilesMigrate{
		api:     api,
		dirPath: dirPath,
	}
}

func (v *FilesMigrate) Read(ctx context.Context) (schemas []Schema, err error) {
	for _, path := range v.dirPath {
		var files []os.DirEntry
		if files, err = os.ReadDir(path); err != nil {
			return nil, err
		}
		for _, f := range files {
			switch filepath.Ext(f.Name()) {
			case ".json", ".yaml", ".yml":
				if s, err := v.read(path, f); err != nil {
					return nil, err
				} else if len(s) > 0 {
					schemas = append(schemas, s...)
				}
			}
		}
	}
	return
}

func (v *FilesMigrate) Run(ctx context.Context, save SaveFunc) (err error) {
	for _, path := range v.dirPath {
		var files []os.DirEntry
		if files, err = os.ReadDir(path); err != nil {
			return err
		}
		for _, f := range files {
			if strings.HasPrefix(f.Name(), "_") {
				continue
			}
			switch filepath.Ext(f.Name()) {
			case ".json", ".yaml", ".yml":
				if err = v.migrate(ctx, v.api, path, f, save); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type Schema struct {
	name    string
	Table   TableSchema              `json:"schema" yaml:"schema"`
	Records []map[string]interface{} `json:"records" yaml:"records"`
}

type Migration struct {
	ID string `json:"id" yaml:"id" dynamodbav:"id"`
}

//var ErrNotFound *types.ResourceNotFoundException

func IsNotFound(err error) bool {
	var notFound *types.ResourceNotFoundException
	return errors.As(err, &notFound)
}

const MigrationTable = "dynamo_migrations"

func createMigrationTable(ctx context.Context, api MigrationApi) error {
	attributes := []types.AttributeDefinition{
		{
			AttributeName: aws.String("id"),
			AttributeType: types.ScalarAttributeTypeS,
		},
	}
	keys := []types.KeySchemaElement{{
		AttributeName: aws.String("id"),
		KeyType:       types.KeyTypeHash,
	}}
	if _, err := api.CreateTable(ctx, &dynamodb.CreateTableInput{
		AttributeDefinitions: attributes,
		KeySchema:            keys,
		BillingMode:          types.BillingModePayPerRequest,
		TableName:            aws.String(MigrationTable),
	}); err != nil {
		return err
	}
	return nil
}

func migrated(ctx context.Context, api MigrationApi, name string) (bool, error) {
	out, err := api.GetItem(ctx, &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: name},
		},
		TableName: aws.String(MigrationTable),
	})
	if err != nil {
		if !IsNotFound(err) {
			return false, err
		}
	}
	return out != nil && len(out.Item) > 0, nil
}

func (v *FilesMigrate) read(path string, file os.DirEntry) (schemas []Schema, err error) {
	name := file.Name()
	if body, err := os.ReadFile(filepath.Join(path, name)); err != nil {
		return nil, errors.WithStack(err)
	} else {
		if strings.HasSuffix(name, ".json") {
			if schemas, err = ParseJson(name, body); err != nil {
				return nil, errors.WithStack(err)
			}
		} else if strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml") {
			if schemas, err = ParseYaml(name, body); err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}
	return
}

func (v *FilesMigrate) migrate(ctx context.Context, api MigrationApi, path string, file os.DirEntry, save SaveFunc) error {
	if _, err := api.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(MigrationTable)}); err != nil {
		if IsNotFound(err) { // テーブルが存在しない場合
			if err = createMigrationTable(ctx, api); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	out, err := api.GetItem(ctx, &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: file.Name()},
		},
		TableName: aws.String(MigrationTable),
	})
	if err != nil {
		if !IsNotFound(err) {
			return err
		}
	}
	if out == nil || out.Item == nil {
		log.Info(ctx).Msgf("%s start", file.Name())
		defer log.Info(ctx).Msgf("%s end", file.Name())
		if err = v.createTable(ctx, api, path, file.Name(), save); err != nil {
			return err
		}
	}
	return nil
}

func (v *FilesMigrate) createTable(ctx context.Context, api MigrationApi, path, name string, save SaveFunc) error {
	var schemas []Schema
	if body, err := os.ReadFile(filepath.Join(path, name)); err != nil {
		return errors.WithStack(err)
	} else {
		if strings.HasSuffix(name, ".json") {
			if schemas, err = ParseJson(name, body); err != nil {
				return errors.WithStack(err)
			}
		} else if strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml") {
			if schemas, err = ParseYaml(name, body); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	for _, s := range schemas {
		if ok, err := migrated(ctx, api, s.name); err != nil {
			return err
		} else if !ok {
			if out, err := s.Table.Exists(ctx, api); err != nil {
				return err
			} else if out != nil { // テーブルが存在する場合、更新
				if _, err = s.Table.Update(ctx, api, *out.Table); err != nil { // TODO 検証
					return err
				}
			} else { // テーブルが存在しない場合、作成
				if _, err = s.Table.Create(ctx, api); err != nil {
					return err
				}
			}
			if save != nil {
				for _, r := range s.Records {
					if err := save(ctx, api, s.Table.TableName, convertValue(r)); err != nil {
						return err
					}
				}
			}
			if err := saveMigration(ctx, api, s.name); err != nil {
				return err
			}
		}
	}
	return nil
}

func saveMigration(ctx context.Context, api MigrationApi, name string) (err error) {
	var item map[string]types.AttributeValue
	if item, err = attributevalue.MarshalMap(&Migration{ID: name}); err != nil {
		return errors.WithStack(err)
	}
	if _, err = api.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(MigrationTable),
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type SaveFunc func(ctx context.Context, api MigrationApi, tableName string, record map[string]interface{}) error
