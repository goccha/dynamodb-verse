package gen

import (
	"context"
	"testing"
)

func TestGenerate(t *testing.T) {
	type args struct {
		path        string
		packageName string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"test1", args{"../../examples", "github.com/goccha/examples/models"}, false},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res, err := Generate(ctx, FileSource{Path: tt.args.path},
				WithPackageName(tt.args.packageName),
				WithEntityPackage("models"),
				WithTablePackage("models"),
			); (err != nil) != tt.wantErr {
				t.Errorf("Generate() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				t.Logf("Generate() = %v", res)
			}
		})
	}
}
