package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/goccha/dynamodb-verse/pkg/foundations"
	"github.com/goccha/dynamodb-verse/pkg/migrate"
	"github.com/goccha/envar"
)

var (
	version  = "v0.0.0"
	revision = "0000000"
)

func main() {
	var local, ver, debug bool
	var region, endpoint, profile, dirPath string
	flag.StringVar(&region, "region", "", "AWS Region")
	flag.StringVar(&endpoint, "endpoint", "", "AWS DynamoDB Endpoint")
	flag.StringVar(&profile, "profile", "", "AWS Profile")
	flag.StringVar(&dirPath, "path", "", "Directory path for configuration files")
	flag.BoolVar(&local, "local", true, "for dynamodb-local")
	flag.BoolVar(&ver, "version", false, "show version")
	flag.BoolVar(&debug, "debug", false, "debug mode")
	flag.Parse()

	if ver {
		fmt.Printf("%s\n", Version())
		return
	}
	ctx := context.Background()
	var err error
	if region == "" {
		region = envar.Get("AWS_REGION", "AWS_DEFAULT_REGION").String("ap-northeast-1")
	}
	if profile == "" {
		profile = envar.String("AWS_PROFILE")
	}
	if endpoint == "" {
		if local {
			endpoint = "http://localhost:8000"
		} else {
			endpoint = envar.String("AWS_DYNAMODB_ENDPOINT")
		}
	}
	cli, err := foundations.Setup(ctx, &foundations.DefaultBuilder{
		Config: &foundations.Config{
			Debug:    debug || envar.Bool("AWS_DEBUG_LOG"),
			Region:   region,
			Endpoint: endpoint,
			Profile:  profile,
		},
	})
	if err != nil {
		panic(err)
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
