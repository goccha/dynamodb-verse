package migrate

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

func Setup(ctx context.Context, conf Config) (MigrationApi, error) {
	var logLevel aws.ClientLogMode
	if conf.Debug || envar.Bool("AWS_DEBUG_LOG") {
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
	endpoint := conf.Endpoint
	if conf.Local && endpoint == "" {
		endpoint = "http://localhost:8000"
	}
	if endpoint = envar.Get("AWS_DYNAMODB_ENDPOINT").String(endpoint); endpoint != "" { // dynamodb-local対応
		if conf.Debug {
			fmt.Printf("endpoint=%s\n", endpoint)
		}
		cli = dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	} else {
		if conf.Debug {
			fmt.Printf("endpoint=default\n")
		}
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
