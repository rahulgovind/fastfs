package helpers

import (
	"bufio"
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// Block Upload code
func (c *Client) putBlock(filepath string, block int64, data []byte) error {
	for {
		target := c.cmap.Get(fmt.Sprintf("%s:%d", filepath, block))

		url := fmt.Sprintf("http://%s/put/%s?block=%d", target, filepath, block)

		maxRetries := 3
		numRetries := 0

		log.Println("Client put")

		buffer := bytes.NewBuffer(data)
		req, err := http.NewRequest("PUT", url, buffer)

		if err == io.EOF {
			log.Fatal("Tried to upload empty block")
			return err
		}

		if err != nil {
			numRetries += 1
			if numRetries <= maxRetries {
				time.Sleep(2 * time.Second)
				continue
			}
			log.Fatal(err)
			return err
		}

		client := &http.Client{}
		res, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}

		res.Body.Close()
		break
	}
	return nil
}

func (c *Client) finalizeBlocks(filepath string, numBlocks int64, numWritten int64) error {
	if numBlocks == 0 {
		return nil
	}

	for {
		target := c.cmap.Get(fmt.Sprintf("%d", rand.Intn(1024*1024)))

		url := fmt.Sprintf("http://%s/confirm/%s?numblocks=%d&numwritten=%d", target, filepath, numBlocks, numWritten)

		maxRetries := 3
		numRetries := 0

		log.Info("Client confirm blocks")
		req, err := http.NewRequest("GET", url, nil)

		if err != nil {
			numRetries += 1
			if numRetries <= maxRetries {
				time.Sleep(2 * time.Second)
				continue
			}
			log.Fatal(err)
			return err
		}

		client := &http.Client{}
		res, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}

		res.Body.Close()
		break
	}
	return nil
}

func (c *Client) blockUploader() {
	for {
		input := <-c.S3UploadChan
		c.putBlock(input.filepath, input.block, input.data)
		input.wg.Done()
		<-input.sem
	}
}

type BlockUploadWriter struct {
	writer    io.WriteCloser
	bufWriter *bufio.Writer
	filePath  string
	wg        sync.WaitGroup
	client    *Client
	done      chan bool
}

func (c *Client) BlockUploadWriter(filePath string) (io.WriteCloser, error) {
	u := new(BlockUploadWriter)

	reader, writer := io.Pipe()
	u.writer = writer
	u.bufWriter = bufio.NewWriterSize(u.writer, 1024*1024)
	u.filePath = filePath
	u.done = make(chan bool)
	u.client = c

	go u.readFrom(reader)
	return u, nil
}

func (u *BlockUploadWriter) Write(b []byte) (int, error) {
	return u.bufWriter.Write(b)
}

func (u *BlockUploadWriter) Close() (err error) {
	log.Info("Calling close")

	err = u.bufWriter.Flush()
	if err != nil {
		log.Fatal(err)
		return
	}

	err = u.writer.Close()
	if err != nil {
		return
	}

	// Must wait for all uploads to finish and confirmation to be sent over :)
	log.Info("Done?")
	<-u.done
	return nil
}

func (u *BlockUploadWriter) addUpload(sem chan bool, block int64, buf *bytes.Buffer) {
	u.wg.Add(1)
	u.client.S3UploadChan <- &BlockUploadInput{u.filePath, block, buf.Bytes(),
		sem, &u.wg}
}

func (u *BlockUploadWriter) waitForAllBlockUploads() {
	u.wg.Wait()
}

func (u *BlockUploadWriter) readFrom(reader io.Reader) {
	blockSize := u.client.BlockSize
	nextUpload := int64(0)
	sem := make(chan bool, 128)
	n := int64(0)

	for {

		sem <- true
		buf := bytes.NewBuffer(nil)

		ni, readErr := io.CopyN(buf, reader, blockSize)
		n += ni
		if readErr != nil && readErr != io.EOF {
			log.Fatal(readErr)
		}

		log.Info("Add upload: ", nextUpload)
		u.addUpload(sem, nextUpload, buf)

		nextUpload += 1
		if readErr == io.EOF {
			break
		}
	}
	log.Info("Waiting for all blocks")
	u.waitForAllBlockUploads()
	log.Info("Finalizing blocks")
	u.client.finalizeBlocks(u.filePath, nextUpload, n)
	u.done <- true
	log.Info("Done!")
}
