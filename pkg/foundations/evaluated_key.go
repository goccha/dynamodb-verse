package foundations

import (
	"encoding/base64"
	"encoding/json"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

type EvaluatedKey map[string]types.AttributeValue
type EvaluatedValue struct {
	Type  string `json:"t"`
	Value string `json:"v"`
}

func (ek EvaluatedKey) String() (string, error) {
	m := make(map[string]EvaluatedValue, 2)
	for k, v := range ek {
		switch val := v.(type) {
		case *types.AttributeValueMemberS:
			m[k] = EvaluatedValue{
				Type:  "s",
				Value: val.Value,
			}
		case *types.AttributeValueMemberN:
			m[k] = EvaluatedValue{
				Type:  "n",
				Value: val.Value,
			}
		case *types.AttributeValueMemberBOOL:
			m[k] = EvaluatedValue{
				Type:  "b",
				Value: strconv.FormatBool(val.Value),
			}
		}
	}
	bin, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bin), nil
}

func EvaluatedKeyOf(key string) (EvaluatedKey, error) {
	bin, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		return nil, err
	}
	m := make(map[string]EvaluatedValue, 2)
	if err = json.Unmarshal(bin, &m); err != nil {
		return nil, err
	}
	values := make(EvaluatedKey, len(m))
	for k, v := range m {
		switch v.Type {
		case "s":
			values[k] = &types.AttributeValueMemberS{Value: v.Value}
		case "n":
			values[k] = &types.AttributeValueMemberN{Value: v.Value}
		case "b":
			values[k] = &types.AttributeValueMemberBOOL{Value: v.Value == "true"}
		default:
			return nil, errors.Errorf("unsupported type: %T", v)
		}
	}
	return values, nil
}
