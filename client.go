package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	BlockSize  int
	ServerAddr string
}

func NewClient(ServerAddr string, BlockSize int) *Client {
	c := new(Client)
	c.ServerAddr = ServerAddr
	c.BlockSize = BlockSize
	return c
}

func (c *Client) GetBlockSize() int {
	return c.BlockSize
}

func (c *Client) Get(path string, block int) ([]byte, error) {
	url := fmt.Sprintf("http://%s/data/%s?block=%d", c.ServerAddr, path, block)

	maxRetries := 3
	numRetries := 0
	var buffer *bytes.Buffer

	for {
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
		break
	}

	return buffer.Bytes(), nil
}
