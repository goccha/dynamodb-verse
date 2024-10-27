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
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
)

type Config struct {
	Local          bool
	Region         string
	Endpoint       string
	Profile        string
	Debug          bool
	EnabledTracing bool
	Cfg            *aws.Config
}

func (c *Config) apply(option ...ConfigOption) *Config {
	for _, o := range option {
		o(c)
	}
	return c
}

type ConfigOption func(*Config)

func Local() ConfigOption {
	return func(c *Config) {
		c.Local = true
	}
}

func Region(region string) ConfigOption {
	return func(c *Config) {
		c.Region = region
	}
}

func Endpoint(endpoint string) ConfigOption {
	return func(c *Config) {
		c.Endpoint = endpoint
	}
}

func Profile(profile string) ConfigOption {
	return func(c *Config) {
		c.Profile = profile
	}
}

func Debug() ConfigOption {
	return func(c *Config) {
		c.Debug = true
	}
}

func EnabledTracing() ConfigOption {
	return func(c *Config) {
		c.EnabledTracing = true
	}
}

func AwsConfig(cfg *aws.Config) ConfigOption {
	return func(c *Config) {
		c.Cfg = cfg
	}
}

func Setup(ctx context.Context, option ...ConfigOption) (cli *dynamodb.Client, err error) {
	conf := new(Config).apply(option...)
	var logLevel aws.ClientLogMode
	var cfg aws.Config
	if conf.Cfg != nil {
		cfg = *conf.Cfg
	} else {
		if conf.Debug {
			logLevel = aws.LogSigning | aws.LogRequestWithBody | aws.LogRetries | aws.LogResponseWithBody
		}
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(conf.Region),
			config.WithSharedConfigProfile(conf.Profile),
			config.WithClientLogMode(logLevel),
			config.WithLogger(logging.NewStandardLogger(os.Stdout)), config.WithLogConfigurationWarnings(true),
		)
		if err != nil {
			return nil, err
		}
	}
	if !validate(cfg) {
		if conf.Profile != "" {
			return nil, fmt.Errorf("profile '%s' is not defined in the credentials file", conf.Profile)
		}
		return nil, errors.New("default settings are not defined in the credentials file")
	}
	if conf.EnabledTracing {
		// instrument all aws clients
		otelaws.AppendMiddlewares(&cfg.APIOptions)
	}
	if conf.Local { // local mode
		conf.Endpoint = "http://localhost:8000"
	}
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
	Build(ctx context.Context) []ConfigOption
}

type EnvBuilder struct{}

func (b *EnvBuilder) Build(ctx context.Context) (options []ConfigOption) {
	options = make([]ConfigOption, 0, 7)
	if envar.Bool("AWS_DEBUG_LOG") {
		options = append(options, Debug())
	}
	options = append(options, Region(envar.Get("AWS_REGION,AWS_DEFAULT_REGION").String("ap-northeast-1")))
	if v := envar.String("AWS_DYNAMODB_ENDPOINT"); v != "" {
		options = append(options, Endpoint(v))
	}
	if v := envar.String("AWS_PROFILE"); v != "" {
		options = append(options, Profile(v))
	}
	if envar.Bool("AWS_ENABLE_TRACING") {
		options = append(options, EnabledTracing())
	}
	return options
}

type OptionBuilder struct {
	Local          bool
	Region         string
	Endpoint       string
	Profile        string
	Debug          bool
	EnabledTracing bool
	Cfg            *aws.Config
}

func (b *OptionBuilder) Build(ctx context.Context) (options []ConfigOption) {
	options = make([]ConfigOption, 0, 7)
	if b.Local {
		options = append(options, Local())
	}
	if b.Region != "" {
		options = append(options, Region(b.Region))
	}
	if b.Endpoint != "" {
		options = append(options, Endpoint(b.Endpoint))
	}
	if b.Profile != "" {
		options = append(options, Profile(b.Profile))
	}
	if b.Debug {
		options = append(options, Debug())
	}
	if b.EnabledTracing {
		options = append(options, EnabledTracing())
	}
	if b.Cfg != nil {
		options = append(options, AwsConfig(b.Cfg))
	}
	return options
}
