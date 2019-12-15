package s3_download

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	s32 "github.com/rahulgovind/fastfs/s3"
	"io/ioutil"
	"log"
	"time"
)

func main() {
	downloader := s32.GetDownloader()

	bucket := "speedfs"
	path := "parking-citations-500k.csv"
	w := &s32.FakeWriterAt{ioutil.Discard}
	start := time.Now()
	numBytes, err := downloader.Download(
		w,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(path),
		})
	elapsed := time.Since(start)

	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Tokk %v seconds\tSize: %v\tSpeed: %v", elapsed, numBytes, s32.ByteSpeed(numBytes, elapsed))
}
