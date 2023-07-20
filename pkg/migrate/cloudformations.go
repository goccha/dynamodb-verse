package migrate

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

const DynamoDB = "AWS::DynamoDB::Table"

type Template struct {
	Version   string `json:"AWSTemplateFormatVersion" yaml:"AWSTemplateFormatVersion"`
	Resources `json:"Resources" yaml:"Resources"`
}

type Resources map[string]Resource

type Resource struct {
	Type       string      `json:"Type" yaml:"Type"`
	Properties TableSchema `json:"Properties" yaml:"Properties"`
}

func (r *Resource) Checksum() (h [32]byte, err error) {
	b, err := json.Marshal(r)
	if err != nil {
		return h, err
	}
	return sha256.Sum256(b), nil
}

func ParseJson(name string, body []byte) (schemas []Schema, err error) {
	t := Template{}
	if err = json.Unmarshal(body, &t); err != nil {
		return nil, errors.WithStack(err)
	}
	if t.Version == "" {
		s := &Schema{name: name}
		if err = json.Unmarshal(body, s); err != nil {
			return nil, errors.WithStack(err)
		}
		schemas = []Schema{
			*s,
		}
	} else {
		schemas = make([]Schema, 0, len(t.Resources))
		for _, r := range t.Resources {
			if r.Type == DynamoDB {
				h, err := r.Checksum()
				if err != nil {
					return schemas, err
				}
				key := fmt.Sprintf("%s_%s:%x", name, r.Properties.TableName, h)
				schemas = append(schemas, Schema{name: key, Table: r.Properties})
			}
		}
	}
	return
}

func ParseYaml(name string, body []byte) (schemas []Schema, err error) {
	t := Template{}
	if err = yaml.Unmarshal(body, &t); err != nil {
		return nil, errors.WithStack(err)
	}
	if t.Version == "" {
		s := &Schema{name: name}
		if err = yaml.Unmarshal(body, s); err != nil {
			return nil, errors.WithStack(err)
		}
		if s.Table.TableName != "" {
			schemas = []Schema{
				*s,
			}
		}
	} else {
		schemas = make([]Schema, 0, len(t.Resources))
		for _, r := range t.Resources {
			if r.Type == DynamoDB {
				h, err := r.Checksum()
				if err != nil {
					return schemas, err
				}
				key := fmt.Sprintf("%s_%s:%x", name, r.Properties.TableName, h)
				schemas = append(schemas, Schema{name: key, Table: r.Properties})
			}
		}
	}
	return
}
