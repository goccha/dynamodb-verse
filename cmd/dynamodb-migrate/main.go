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
	var ver bool
	var dirPath string
	options := foundations.OptionBuilder{}
	flag.StringVar(&options.Region, "region", "", "AWS Region")
	flag.StringVar(&options.Endpoint, "endpoint", "", "AWS DynamoDB Endpoint")
	flag.StringVar(&options.Profile, "profile", "", "AWS Profile")
	flag.BoolVar(&options.Local, "local", true, "for dynamodb-local")
	flag.BoolVar(&options.Debug, "debug", false, "debug mode")

	flag.StringVar(&dirPath, "path", "", "Directory path for configuration files")
	flag.BoolVar(&ver, "version", false, "show version")
	flag.Parse()

	if ver {
		fmt.Printf("%s\n", Version())
		return
	}
	ctx := context.Background()
	cli, err := foundations.Setup(ctx, options.Build(ctx)...)
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
