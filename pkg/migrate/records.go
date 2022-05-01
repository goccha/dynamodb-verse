package migrate

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/logging/log"
	"github.com/google/uuid"
	"strings"
	"time"
)

func SaveRecord(ctx context.Context, api MigrationApi, tableName string, record map[string]interface{}) (err error) {
	var item map[string]types.AttributeValue
	if item, err = attributevalue.MarshalMap(record); err != nil {
		return
	} else {
		var out *dynamodb.PutItemOutput
		if out, err = api.PutItem(ctx, &dynamodb.PutItemInput{
			Item:      item,
			TableName: &tableName,
		}); err != nil {
			log.Error(ctx).Str("table", tableName).Err(err).Send()
			return fmt.Errorf("%w", err)
		} else {
			log.Debug(ctx).Msgf("%+v", out)
		}
	}
	return
}

func convertValue(record map[string]interface{}) map[string]interface{} {
	m := map[string]interface{}{}
	for k, v := range record {
		switch val := v.(type) {
		case string:
			val = strings.Trim(val, " ")
			if strings.HasPrefix(val, "{{") && strings.HasSuffix(val, "}}") {
				switch val[2 : len(val)-2] {
				case "uuid()":
					m[k] = uuid.NewString()
				case "now()":
					m[k] = time.Now()
				}
			} else {
				m[k] = v
			}
		default:
			m[k] = v
		}
	}
	return m
}
