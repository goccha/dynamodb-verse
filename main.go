package main

import (
	"context"
	"flag"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/logging"
	"github.com/goccha/dynamodb-verse/pkg/migrate"
	"github.com/goccha/envar"
	"os"
)

func main() {
	var region, endpoint, profile, dirPath string
	flag.StringVar(&region, "region", "ap-northeast-1", "AWS Region")
	flag.StringVar(&endpoint, "endpoint", "", "AWS DynamoDB Endpoint")
	flag.StringVar(&profile, "profile", "", "AWS Profile")
	flag.StringVar(&dirPath, "path", "configs/dynamodb", "Directory path for configuration files")

	flag.Parse()

	ctx := context.Background()
	var err error
	var logLevel aws.ClientLogMode
	if envar.Bool("AWS_DEBUG_LOG") {
		logLevel = aws.LogSigning | aws.LogRequestWithBody | aws.LogRetries | aws.LogResponseWithBody
	}
	region = envar.Get("AWS_REGION", "AWS_DEFAULT_REGION").String(region)
	var cfg aws.Config
	if cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(region),
		config.WithSharedConfigProfile(envar.Get("AWS_PROFILE").String(profile)),
		config.WithClientLogMode(logLevel),
		config.WithLogger(logging.NewStandardLogger(os.Stdout)), config.WithLogConfigurationWarnings(true),
	); err != nil {
		return
	}
	var cli *dynamodb.Client
	if endpoint = envar.Get("AWS_DYNAMODB_ENDPOINT").String(endpoint); endpoint != "" { // dynamodb-local対応
		cli = dynamodb.NewFromConfig(cfg, dynamodb.WithEndpointResolver(
			dynamodb.EndpointResolverFromURL(endpoint)),
		)
	} else {
		cli = dynamodb.NewFromConfig(cfg)
	}
	dirPath = envar.Get("DYNAMODB_CONFIG_PATH").String(dirPath)
	if err = migrate.New(cli, dirPath).Run(ctx, migrate.SaveRecord); err != nil {
		panic(err)
	}
}
