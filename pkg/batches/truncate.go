package batches

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/dynamodb-verse/pkg/foundations"
	"github.com/goccha/dynamodb-verse/pkg/foundations/options"
)

type TruncateClient interface {
	foundations.ScanClient
	WriteClient
}

// Truncate Deletes records in the specified table.
func Truncate[T any](ctx context.Context, db TruncateClient, condition foundations.ScanFilterFunc, keyFunc DeleteKeyFunc[T]) error {
	var opt []options.Option
	for {
		b := New()
		out, err := foundations.Scan(ctx, db, condition, func(tableName string, value foundations.Records) error {
			rec := make([]T, len(value))
			if err := value.Unmarshal(ctx, &rec); err != nil {
				return err
			}
			for _, v := range rec {
				b.Delete(func() (table string, item map[string]types.AttributeValue, err error) {
					table = tableName
					item = keyFunc(v)
					return
				})
			}
			return nil
		}, opt...)
		if err != nil {
			return err
		}
		if out.LastEvaluatedKey != nil {
			opt = []options.Option{options.ExclusiveStartKey(out.LastEvaluatedKey)}
		}
		if err = b.Run(ctx, db); err != nil {
			return err
		}
		if len(opt) == 0 {
			break
		}
	}
	return nil
}
