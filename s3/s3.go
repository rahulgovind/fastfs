package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strings"
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
	fmt.Println(result)
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

type FakeWriterAt struct {
	w io.Writer
}

func (fw *FakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return fw.w.Write(p)
}

func GetDownloader() *s3manager.Downloader {
	startTime := time.Now()
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2")},
	)
	downloader := s3manager.NewDownloader(sess)
	elasped := time.Since(startTime)
	fmt.Println("Creating throughput took :", elasped)
	return downloader
}

func DownloadToFileWithDownloader(bucket string, downloader *s3manager.Downloader,
	path string, file io.WriterAt, offset int64, size int64) error {

	var rng string
	if size == -1 {
		rng = "bytes=0-"
	} else {
		rng = fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
	}

	start := time.Now()
	numBytes, err := downloader.Download(
		file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(path),
			Range:  aws.String(rng),
		})

	elapsed := time.Since(start)
	log.Infof("Downloaded %v\tSize: %v bytes\tTime: %v\tSpeed: %v",
		path, numBytes, elapsed,
		ByteSpeed(numBytes, elapsed),
	)

	if err != nil {
		if strings.Contains(err.Error(), "InvalidRange") {
			return nil
		}
		log.Fatalf("Unable to download item %q, %v", path, err)
	}

	return nil
}

func DownloadToFile(bucket string, path string, file io.WriterAt, offset int64, size int64) error {
	return DownloadToFileWithDownloader(bucket, GetDownloader(), path, file, offset, size)
}

func DownloadObject(bucket string, path string, outputFilename string) error {
	file, err := os.Create(outputFilename)
	if err != nil {
		log.Fatalf("Unable to open file %q, %v", err)
	}

	defer file.Close()

	return DownloadToFile(bucket, path, file, 0, -1)
}

func DownloadToWriterPartial(bucket string, downloader *s3manager.Downloader,
	path string, writer io.Writer, offset int64, size int64) error {
	return DownloadToFileWithDownloader(bucket, downloader, path, &FakeWriterAt{writer}, offset, size)
}

func DownloadToWriter(bucket string, path string, writer io.Writer) error {
	return DownloadToWriterPartial(bucket, GetDownloader(), path, writer, 0, -1)
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

func CopyObject(bucket string, src string, dest string) error {
	svc := getS3Client(bucket)

	result, err := svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String("/" + bucket + "/" + src),
		Key:        aws.String(dest),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeObjectNotInActiveTierError:
				log.Error(s3.ErrCodeObjectNotInActiveTierError, aerr.Error())
			default:
				log.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Error(err.Error())
		}
		return err
	}

	log.Info(result)
	return nil
}

func DeleteObject(bucket string, key string) error {
	svc := getS3Client(bucket)

	result, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Error(err.Error())
		}
		return err
	}

	log.Info(result)
	return nil
}

func PutOjbect(bucket string, path string, r io.Reader) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2"),
	})
	uploader := s3manager.NewUploader(sess)

	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
		Body:   r,
		ACL:    aws.String("public-read"),
	})

	if err != nil {
		log.Fatal(err)
		return fmt.Errorf("failed to upload file, %v", err)
	}
	fmt.Printf("file uploaded to, %s\n", result.Location)

	return nil
}

func MoveObject(bucket string, src string, dest string) error {
	err := CopyObject(bucket, src, dest)
	if err != nil {
		return err
	}
	err = DeleteObject(bucket, src)
	return err
}
