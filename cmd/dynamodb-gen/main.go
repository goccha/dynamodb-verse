package main

import (
	"context"
	"flag"

	"github.com/goccha/dynamodb-verse/pkg/gen"
)

func main() {
	type arguments struct {
		SrcPath       string
		DestPath      string
		PackageName   string
		EntityPackage string
		TablePackage  string
	}
	args := arguments{}
	flag.StringVar(&args.SrcPath, "src", "", "CloudFormation file path")
	flag.StringVar(&args.DestPath, "dest", "", "Destination path")
	flag.StringVar(&args.PackageName, "package", "", "Package name")
	flag.StringVar(&args.EntityPackage, "entities", "", "Entity package name")
	flag.StringVar(&args.TablePackage, "tables", "", "Table package name")

	flag.Parse()

	ctx := context.Background()
	if res, err := gen.Generate(ctx, gen.FileSource{Path: args.SrcPath}, gen.Options{
		PackageName:   args.PackageName,
		EntityPackage: args.EntityPackage,
		TablePackage:  args.TablePackage,
	}); err != nil {
		panic(err)
	} else {
		if err = res.WriteFile(ctx, args.DestPath); err != nil {
			panic(err)
		}
	}
}
