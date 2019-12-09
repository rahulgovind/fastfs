package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
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
	url := fmt.Sprintf("http://%s/data/%s?block=%d?force=1", c.ServerAddr, path, block)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	buffer := new(bytes.Buffer)
	_, err = io.Copy(buffer, resp.Body)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
