package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/goccha/dynamodb-verse/pkg/migrate"
	"github.com/goccha/envar"
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
	if region == "" {
		region = envar.Get("AWS_REGION", "AWS_DEFAULT_REGION").String("ap-northeast-1")
	}
	if profile == "" {
		profile = envar.String("AWS_PROFILE")
	}
	cli, err := migrate.Setup(ctx, migrate.Config{
		Local:    local,
		Region:   region,
		Endpoint: endpoint,
		Profile:  profile,
		Debug:    strings.EqualFold(debug, "true") || debug == "" && envar.Bool("AWS_DEBUG_LOG"),
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
