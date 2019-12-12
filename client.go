package main

import (
	"bytes"
	"fmt"
	"github.com/rahulgovind/fastfs/datamanager"
	"io"
	"log"
	"net/http"
	"time"
)

type Client struct {
	BlockSize   int64
	ServerAddr  string
	dm          *datamanager.DataManager
	partitioner Partitioner
}

func NewClient(ServerAddr string, BlockSize int64, dm *datamanager.DataManager, partitioner Partitioner) *Client {
	c := new(Client)
	c.ServerAddr = ServerAddr
	c.BlockSize = BlockSize
	c.dm = dm
	c.partitioner = partitioner
	return c
}

func (c *Client) GetBlockSize() int64 {
	return c.BlockSize
}

func (c *Client) Get(path string, block int64) ([]byte, error) {
	for {
		addr := c.partitioner.GetServer(path, int(block))
		if addr == c.ServerAddr {
			return c.dm.Get(path, block)
		}

		url := fmt.Sprintf("http://%s/data/%s?block=%d&force=1", addr, path, block)

		maxRetries := 3
		numRetries := 0
		var buffer *bytes.Buffer

		resp, err := http.Get(url)
		if err != nil {
			numRetries += 1
			if numRetries <= maxRetries {
				time.Sleep(2 * time.Second)
				continue
			}
			return nil, err
		}

		buffer = new(bytes.Buffer)
		_, err = io.Copy(buffer, resp.Body)
		if err != nil {
			numRetries += 1
			if numRetries <= maxRetries {
				time.Sleep(2 * time.Second)
				continue
			}
			return nil, err
		}

		defer resp.Body.Close()
		return buffer.Bytes(), nil
	}
}

func (c *Client) Put(path string, block int64, data []byte) error {
	url := fmt.Sprintf("http://%s/put/%s?block=%d", c.ServerAddr, path, block)

	maxRetries := 3
	numRetries := 0
	buffer := bytes.NewBuffer(data)

	log.Println("Client put")

	for {
		req, err := http.NewRequest("PUT", url, buffer)

		if err == io.EOF {
			break
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

		defer res.Body.Close()
		break
	}
	return nil
}
