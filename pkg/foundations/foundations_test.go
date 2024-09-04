package foundations

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type TestItem struct {
	ID     string `json:"id" dynamodbav:"id"`
	Called bool   `json:"called"`
}

func (item *TestItem) AfterFetchItem(ctx context.Context) error {
	fmt.Printf("after fetch item %s\n", item.ID)
	item.Called = true
	return nil
}

type TestItems []TestItem

var called bool

func (items TestItems) AfterFetchItems(ctx context.Context) error {
	fmt.Printf("after fetch items\n")
	called = true
	for i, item := range items {
		item.Called = true
		items[i] = item
	}
	return nil
}

func TestRecordsUnmarshal(t *testing.T) {
	ctx := context.Background()
	records := make(Records, 3)
	for i, v := range []string{"1", "2", "3"} {
		m := map[string]types.AttributeValue{}
		m["id"] = &types.AttributeValueMemberN{Value: v}
		records[i] = m
	}
	var items1 []TestItem
	if err := records.Unmarshal(ctx, &items1); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("called is true")
	}
	for _, item := range items1 {
		if item.Called {
			t.Fatal("item.Called is true")
		}
	}
	called = false
	items2 := TestItems{}
	if err := records.Unmarshal(ctx, &items2); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("called is false")
	}
	for _, item := range items2 {
		if !item.Called {
			t.Fatal("item.Called is false")
		}
	}
}
