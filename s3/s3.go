package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

type S3Node struct {
	Path        string
	Size        int64
	IsDirectory bool
}

func PrintBuckets() error {
	s, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	})

	client := s3.New(s)
	result, err := client.ListBuckets(nil)
	if err != nil {
		log.Fatal("Unable to list buckets, ", err)
	}

	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}
	return nil
}

func getS3Client(bucket string) *s3.S3 {
	s, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2"),
	})

	if err != nil {
		log.Fatal(err)
	}
	svc := s3.New(s)
	return svc
}

func ListDirectories(bucket string, path string) []string {
	svc := getS3Client(bucket)
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(path),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		log.Fatalf("Unable to list items in bucket %q, %v", bucket, err)
	}
	var result []string
	for _, prefix := range resp.CommonPrefixes {
		result = append(result, *prefix.Prefix)
	}
	return result
}

func ListFiles(bucket string, path string) []string {
	svc := getS3Client(bucket)
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(path),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		log.Fatalf("Unable to list items in bucket %q, %v", bucket, err)
	}

	var result []string
	for _, item := range resp.Contents {
		result = append(result, *item.Key)
	}
	return result
}

func ListNodes(bucket string, path string) []S3Node {
	svc := getS3Client(bucket)
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(path),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		log.Fatalf("Unable to list items in bucket %q, %v", bucket, err)
	}

	var result []S3Node
	for _, prefix := range resp.CommonPrefixes {
		result = append(result, S3Node{
			Path:        *prefix.Prefix,
			Size:        0,
			IsDirectory: true,
		})
	}

	for _, item := range resp.Contents {
		result = append(result, S3Node{
			Path:        *item.Key,
			Size:        *item.Size,
			IsDirectory: false,
		})
	}
	return result
}

func ByteSize(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func ByteSpeed(numBytes int64, elapsed time.Duration) string {
	speed := int64(float64(numBytes*1e9) / float64(elapsed))

	const unit = 1000
	if speed < unit {
		return fmt.Sprintf("%d B/s", speed)
	}
	div, exp := int64(unit), 0
	for n := speed / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB/s",
		float64(speed)/float64(div), "kMGTPE"[exp])
}

func DownloadObject(bucket string, path string, outputFilename string) error {
	file, err := os.Create(outputFilename)
	if err != nil {
		log.Fatalf("Unable to open file %q, %v", err)
	}

	defer file.Close()

	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2")},
	)

	start := time.Now()
	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(path),
		})

	if err != nil {
		log.Fatalf("Unable to download item %q, %v", path, err)
	}

	elapsed := time.Since(start)

	log.Infof("Downloaded %v\tSize: %v bytes\tTime: %v\tSpeed: %v",
		file.Name(), numBytes, elapsed,
		ByteSpeed(numBytes, elapsed),
	)
	return nil
}

func PrintObjects(bucket string, prefix string, delimiter string) {
	svc := getS3Client(bucket)
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		log.Fatalf("Unable to list items in bucket %q, %v", bucket, err)
	}

	fmt.Println(resp.CommonPrefixes)
	for _, item := range resp.Contents {
		fmt.Println("Name:         ", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:         ", *item.Size)
		fmt.Println("Storage class:", *item.StorageClass)
		fmt.Println("")
	}

	fmt.Println("Found", len(resp.Contents), "items in bucket", bucket)
	fmt.Println("")
}
