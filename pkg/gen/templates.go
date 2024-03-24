package gen

const EntityTemplate = `
{{- if ne .TablePackage .EntityPackage -}}
package {{ .EntityPackage }}
{{ end }}
{{ if .BinaryMarshaller -}}
import "encoding/json"
{{ end }}
type {{ .EntityName }} struct {
    {{- range .Fields }}
	{{ .Name }} {{ .Type }} {{ .BackQuote }}json:"{{ .JsonKey }}" dynamodbav:"{{ .ColumnName }}"{{ .BackQuote }}{{ end }}
	{{ if .TimeToLive }}{{ .TimeToLive.Name }} int64 {{ .BackQuote }}json:"{{ .TimeToLive.JsonKey }}" dynamodbav:"{{ .TimeToLive.ColumnName }}"{{ .BackQuote }}{{ end }} 
}
{{- if .BinaryMarshaller }}
func ({{- .Receiver }} *{{ .EntityName }}) MarshalBinary() ([]byte, error) {
	return json.Marshal({{ .Receiver }})
}
func ({{- .Receiver }} *{{ .EntityName }}) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, {{ .Receiver }})
}
{{ end -}}

`

const TableTemplate = `
{{- $DaoName := printf "%s%s" .EntityName "Dao" -}}
package {{ .TablePackage }}

import (
    "context"
{{ if .BinaryMarshaller -}}
    "encoding/json"
{{ end }}
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/goccha/dynamodb-verse/pkg/foundations"
{{- if ne .TablePackage .EntityPackage }}
	"{{ .PackageName }}/entities"
	{{- $DaoName = .EntityName -}}
{{ end }}
)

type {{ $DaoName }} struct {
    DB foundations.Client {{ .BackQuote }}json:"-" dynamodbav:"-"{{ .BackQuote }}
{{- if ne .TablePackage .EntityPackage }}
	entities.{{ .EntityName }}
{{- else }}
	{{ .EntityName }}
{{- end }}
    UpdateCnt int {{ .BackQuote }}json:"-" dynamodbav:"update_cnt"{{ .BackQuote }}
}

{{- if .BinaryMarshaller }}
func (rec *{{ $DaoName }}) MarshalBinary() ([]byte, error) {
	return json.Marshal(rec)
}
func (rec *{{ $DaoName }}) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, rec)
}
{{ end -}}

func (rec *{{ $DaoName }}) TableName() string {
    return "{{ .TableFullName }}"
}

func (rec *{{ $DaoName }}) GetKey(ctx context.Context) foundations.GetKeyFunc {
    return func() (table string, keys map[string]types.AttributeValue, attrs []string, err error) {
        keys = map[string]types.AttributeValue {
            {{ range .Keys }}"{{ .ColumnName }}": &{{ .Type }}{Value: rec.{{ .FieldName }} },{{ end }}
        }
        table = rec.TableName()
        return
    }
}

func (rec *{{ $DaoName }}) PutItem(ctx context.Context) foundations.WriteItemFunc {
	return foundations.PutItem(ctx, rec.TableName(), rec)
}

func (rec *{{ $DaoName }}) DeleteItem(ctx context.Context) foundations.WriteItemFunc {
	return foundations.DeleteItem(rec.GetKey(ctx))
}

func (rec *{{ $DaoName }}) UpdateItem(ctx context.Context, fields ...foundations.UpdateField) foundations.WriteItemFunc {
	return foundations.UpdateItem(ctx, rec.GetKey(ctx), fields...)
}

func (rec *{{ $DaoName }}) Get(ctx context.Context) (res *{{ $DaoName }}, err error) {
	if _, err = foundations.Get(ctx, rec.DB, rec.GetKey(ctx), foundations.FetchItem(ctx, rec)); err != nil {
		return rec, err
	}
	return rec, nil
}
{{- $PluralDaoName := printf "%s%s" .EntitiesName "Dao" -}}
{{- if ne .TablePackage .EntityPackage }}
	{{- $PluralDaoName = .EntitiesName -}}
{{ end }}
type {{ $PluralDaoName }} []{{ $DaoName }}

`
