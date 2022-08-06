package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/logging"
	"github.com/goccha/dynamodb-verse/pkg/migrate"
	"github.com/goccha/envar"
	"os"
	"strings"
)

var (
	version  = "v0.0.0"
	revision = "0000000"
)

func main() {
	var local, ver bool
	var debug string
	var region, endpoint, profile, dirPath string
	flag.StringVar(&region, "region", "", "AWS Region")
	flag.StringVar(&endpoint, "endpoint", "", "AWS DynamoDB Endpoint")
	flag.StringVar(&profile, "profile", "", "AWS Profile")
	flag.StringVar(&dirPath, "path", "", "Directory path for configuration files")
	flag.BoolVar(&local, "local", true, "for dynamodb-local")
	flag.BoolVar(&ver, "version", false, "show version")
	flag.StringVar(&debug, "debug", "", "debug mode (true|false)")

	flag.Parse()

	if ver {
		fmt.Printf("%s", Version())
		return
	}
	ctx := context.Background()
	var err error
	var logLevel aws.ClientLogMode
	if strings.EqualFold(debug, "true") || debug == "" && envar.Bool("AWS_DEBUG_LOG") {
		logLevel = aws.LogSigning | aws.LogRequestWithBody | aws.LogRetries | aws.LogResponseWithBody
	}
	if region == "" {
		region = envar.Get("AWS_REGION", "AWS_DEFAULT_REGION").String("ap-northeast-1")
	}
	if profile == "" {
		profile = envar.String("AWS_PROFILE")
	}
	var cfg aws.Config
	if cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(region),
		config.WithSharedConfigProfile(profile),
		config.WithClientLogMode(logLevel),
		config.WithLogger(logging.NewStandardLogger(os.Stdout)), config.WithLogConfigurationWarnings(true),
	); err != nil {
		panic(err)
	}
	if !validate(cfg) {
		if profile != "" {
			fmt.Printf("Profile '%s' is not defined in the credentials file.", profile)
		} else {
			fmt.Printf("Default settings are not defined in the credeintials file.")
		}
		os.Exit(1)
	}
	var cli *dynamodb.Client
	if local && endpoint == "" {
		endpoint = "http://localhost:8000"
	}
	if endpoint = envar.Get("AWS_DYNAMODB_ENDPOINT").String(endpoint); endpoint != "" { // dynamodb-local対応
		if strings.EqualFold(debug, "true") {
			fmt.Printf("endpoint=%s\n", endpoint)
		}
		cli = dynamodb.NewFromConfig(cfg, dynamodb.WithEndpointResolver(
			dynamodb.EndpointResolverFromURL(endpoint)),
		)
	} else {
		if strings.EqualFold(debug, "true") {
			fmt.Printf("endpoint=default\n")
		}
		cli = dynamodb.NewFromConfig(cfg)
	}
	if dirPath == "" {
		dirPath = envar.Get("DYNAMODB_CONFIG_PATH").String("configs/dynamodb")
	}
	if err = migrate.New(cli, dirPath).Run(ctx, migrate.SaveRecord); err != nil {
		panic(err)
	}
}

func Version() string {
	return fmt.Sprintf("%s-%s", strings.ReplaceAll(version, "/", "_"), revision)
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
