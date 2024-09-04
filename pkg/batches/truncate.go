package batches

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/dynamodb-verse/pkg/foundations"
)

type TruncateClient interface {
	foundations.ScanClient
	WriteClient
}

// Truncate Deletes records in the specified table.
func Truncate[T any](ctx context.Context, db TruncateClient, condition foundations.ScanFilterFunc, keyFunc DeleteKeyFunc[T]) error {
	b := New()
	_, err := foundations.ScanAll(ctx, db, condition, func(tableName string, value foundations.Records) error {
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
	})
	if err != nil {
		return err
	}
	return b.Run(ctx, db)
}
