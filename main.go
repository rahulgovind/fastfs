package main

import (
	"github.com/rahulgovind/fastfs/cache/hybridcache"
	"github.com/rahulgovind/fastfs/datamanager"
	"github.com/rahulgovind/fastfs/s3"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"os"
)

func main() {
	//defer profile.Start(profile.MemProfile).Stop()
	var bucket string
	var port int

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
		&cli.IntFlag{
			Name:        "port",
			Usage:       "Port to use for membership service",
			Destination: &port,
			Value:       8000,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

	hc := hybridcache.NewHybridCache(32, 1024, 1024*1024, "testdata")
	dm := datamanager.New(bucket, 5, hc, 1024*1024)
	mm := s3.NewS3MetadataManager(bucket)
	partitioner := NewHashPartitioner()
	fastfs := NewFastFS(port, partitioner)

	//config := memberlist.DefaultLocalConfig()
	//
	//config.BindPort = port
	//config.AdvertisePort = port
	//config.Name = fmt.Sprintf("localhost:%v", 8081)
	//config.Events = partitioner
	//list, err := memberlist.Create(config)
	//fmt.Println(list)

	s := NewServer("localhost", 8081, dm, mm, partitioner, fastfs)
	s.Serve()
	//s.LoadServer("", 8081)

	//start := time.Now()
	////dc := hybridcache.NewHybridCache(1024, 1024, 1024, "testdata")
	//dc := diskcache.NewDiskCache(1024*1024, 8*102, "testdata")
	//data := make([]byte, 8*1024)
	//for i := 0; i < 1024*1024; i++ {
	//	dc.Add(fmt.Sprintf("key%d", i), data)
	//	//runtime.GC()
	//}
	//
	////val, ok := dc.Get("key1023")
	////fmt.Println(val, ok)
	//
	//elapsed := time.Since(start)
	//fmt.Println("Total time taken: ", elapsed)
	//hashRing := consistenthash.New(5, nil)
	//
	//list, err := memberlist.Create(config)
	//
	//// Join an existing cluster by specifying at least one known member.
	//fmt.Println(port)
	//if port != 8000 {
	//	list.Join([]string{fmt.Sprintf("127.0.0.1:8000")})
	//}
	//
	////n, err := list.Join([]string{})
	////fmt.Println(n)
	//if err != nil {
	//	panic("Failed to join cluster: " + err.Error())
	//}
	//
	//for _, member := range list.Members() {
	//	fmt.Printf("Member: %s %s %d\n", member.Name, member.Addr, member.Port)
	//}

	//mm := metadatamanager.NewMetadataManager(bucket)
	//mm.LoadFS()

	//s := datamanager.New(bucket, 5)
	//go s.LoadServer("", 8081)
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
	//var wg sync.WaitGroup
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
	//wg.Add(1)
	//go func() {
	//	buf := bytes.NewBuffer(make([]byte, 0, 360*1024*1024))
	//	//var buf bytes.Buffer
	//	s3.DownloadToWriter(bucket, "file2/random1", buf)
	//	wg.Done()
	//}()
	//wg.Wait()

}
