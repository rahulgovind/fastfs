package main

import (
	"github.com/rahulgovind/fastfs/datamanager"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"os"
	"sync"
)

func main() {
	var bucket string

	log.SetLevel(log.DebugLevel)
	app := cli.NewApp()
	app.Name = "FastFS Node"
	app.Usage = "Create FastFS Nodepoint"
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "bucket",
			Usage:       "S3 Bucket to use as backing store",
			Destination: &bucket,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

	//mm := metadatamanager.NewMetadataManager(bucket)
	//mm.LoadFS()

	s := datamanager.New(bucket, 5)
	go s.LoadServer("", 8081)
	//go s.Serve()

	//go api.NewServer(7777, bucket).Start()
	////
	//var conn *grpc.ClientConn
	//conn, err = grpc.Dial(":7777", grpc.WithInsecure())
	//if err != nil {
	//	log.Fatalf("did not connect: %s", err)
	//}
	//defer conn.Close()
	//
	//c := api.NewFastFSClient(conn)
	//response, err := c.OpenFile(context.Background(), &api.OpenFileMessage{Path: "foo__fastfs"})
	////response, err := c.ListFiles(context.Background(), &api.FileQuery{Path: ""})
	//if err != nil {
	//	log.Fatalf("Error when calling Server: %s", err)
	//}
	//log.Printf("Response from server: %v", response.Version)
	//
	//response, err = c.OpenFile(context.Background(), &api.OpenFileMessage{Path: "foo__fastfs"})
	////response, err := c.ListFiles(context.Background(), &api.FileQuery{Path: ""})
	//if err != nil {
	//	log.Fatalf("Error when calling Server: %s", err)
	//}
	//log.Printf("Response from server: %v", response.Version)

	//s3.MoveObject(bucket, "file2/0", "file2/3")
	//mm := metadatamanager.NewS3MetadataManager(bucket)
	//mm.PrintTree()
	//for _, file := range s3.ListNodes(bucket, "") {
	//	fmt.Println(file.Path)
	//}
	//inodes := mm.Stat("/")
	//for _, inode := range inodes {
	//	fmt.Println(inode.Key, inode.IsDir, len(inode.Children))
	//}

	//inode := mm.Stat("")
	//fmt.Println(inode.Key)
	//
	//for _, inode2 := range inode.children {
	//	log.Infof("%v %v", inode2.Key, inode2.IsDir)
	//}
	var wg sync.WaitGroup
	//wg.Add(1)
	//go func() {
	//	buf := bytes.NewBuffer(make([]byte, 0, 360*1024*1024))
	//	//var buf bytes.Buffer
	//	s3.DownloadToWriter(bucket, "file2/random1", buf)
	//	wg.Done()
	//}()
	//wg.Add(1)
	//go func() {
	//	buf := bytes.NewBuffer(make([]byte, 0, 360*1024*1024))
	//	//var buf bytes.Buffer
	//	s3.DownloadToWriter(bucket, "file2/random1", buf)
	//	wg.Done()
	//}()
	wg.Add(1)
	//go func() {
	//	buf := bytes.NewBuffer(make([]byte, 0, 360*1024*1024))
	//	//var buf bytes.Buffer
	//	s3.DownloadToWriter(bucket, "file2/random1", buf)
	//	wg.Done()
	//}()
	wg.Wait()

}
