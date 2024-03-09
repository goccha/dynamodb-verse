package foundations

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/logging"
	"github.com/goccha/envar"
	"github.com/pkg/errors"
)

type Config struct {
	Local    bool
	Region   string
	Endpoint string
	Profile  string
	Debug    bool
}

func Setup(ctx context.Context, builder ConfigBuilder) (*dynamodb.Client, error) {
	conf, err := builder.Build(ctx)
	if err != nil {
		return nil, err
	}
	var logLevel aws.ClientLogMode
	if conf.Debug {
		logLevel = aws.LogSigning | aws.LogRequestWithBody | aws.LogRetries | aws.LogResponseWithBody
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(conf.Region),
		config.WithSharedConfigProfile(conf.Profile),
		config.WithClientLogMode(logLevel),
		config.WithLogger(logging.NewStandardLogger(os.Stdout)), config.WithLogConfigurationWarnings(true),
	)
	if err != nil {
		return nil, err
	}
	if !validate(cfg) {
		if conf.Profile != "" {
			return nil, fmt.Errorf("profile '%s' is not defined in the credentials file", conf.Profile)
		}
		return nil, errors.New("default settings are not defined in the credentials file")
	}
	var cli *dynamodb.Client
	if conf.Endpoint != "" { // dynamodb-local対応
		if conf.Debug {
			fmt.Printf("endpoint=%s\n", conf.Endpoint)
		}
		cli = dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(conf.Endpoint)
		})
	} else {
		cli = dynamodb.NewFromConfig(cfg)
	}
	return cli, nil
}

func validate(cfg aws.Config) bool {
	for _, v := range cfg.ConfigSources {
		switch opt := v.(type) {
		case config.LoadOptions:
			if opt.Credentials != nil {
				return true
			}
		case config.EnvConfig:
			if opt.Credentials.AccessKeyID != "" {
				return true
			}
		case config.SharedConfig:
			if opt.Credentials.AccessKeyID != "" {
				return true
			}
		}
	}
	return false
}

type ConfigBuilder interface {
	Build(ctx context.Context) (*Config, error)
}

type DefaultBuilder struct {
	Config *Config
}

func (b *DefaultBuilder) Build(ctx context.Context) (*Config, error) {
	return b.Config, nil
}

type EnvBuilder struct{}

func (b *EnvBuilder) Build(ctx context.Context) (*Config, error) {
	return &Config{
		Debug:    envar.Bool("AWS_DEBUG_LOG"),
		Region:   envar.Get("AWS_REGION,AWS_DEFAULT_REGION").String("ap-northeast-1"),
		Endpoint: envar.Get("AWS_DYNAMODB_ENDPOINT").String(""),
		Profile:  envar.Get("AWS_PROFILE").String(""),
	}, nil
}
