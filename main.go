package main

import (
	"fmt"
	"github.com/rahulgovind/fastfs/s3"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"os"
)

func main() {
	var bucket string

	log.SetLevel(log.DebugLevel)
	app := cli.NewApp()
	app.Name = "FastFS Node"
	app.Usage = "Create FastFS Nodepoint"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "bucket",
			Usage:       "S3 Bucket to use as backing store",
			Destination: &bucket,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

	for _, node := range s3.ListNodes(bucket, "file2/") {
		fmt.Println(node.Path, node.IsDirectory)
	}

	s3.DownloadObject(bucket, "file2/0", "file2-0")
}
