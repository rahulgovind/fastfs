package datamanager

import (
	"github.com/sirupsen/logrus"
	"io"
	"log"
)

type BlockData struct {
	data  []byte
	block int
}

type Aggregator struct {
	path        string
	numParallel int
	getter      BlockGetter
}

func NewAggregator(numParallel int, path string,
	getter BlockGetter) *Aggregator {
	res := new(Aggregator)
	res.getter = getter
	res.path = path
	res.numParallel = numParallel
	return res
}

func (ag *Aggregator) downloadBlock(blockCh chan int, out chan *BlockData) {
	for {
		block := <-blockCh

		logrus.Debugf("look-ahead %v", block)
		if block == -1 {
			break
		}

		data, err := ag.getter.Get(ag.path, block)
		if err != nil {
			log.Fatal(err)
		}

		logrus.Infof("Got data from Get. Block: %v, Length: %v", block, len(data))
		out <- &BlockData{data, block}
	}
}

func (ag *Aggregator) WriteTo(w io.Writer) {
	nextBlock := 0
	nextDownload := 0
	stop := false
	blocks := make(map[int]*BlockData)

	d := make(chan int, ag.numParallel)
	out := make(chan *BlockData, ag.numParallel)

	for i := 0; i < ag.numParallel; i++ {
		d <- nextDownload
		nextDownload += 1
		//log.Infof("Added %v", nextDownload - 1)
		go ag.downloadBlock(d, out)
	}

	for !stop {
		bd := <-out
		logrus.Infof("Got block %v. Length: %v ", bd.block, len(bd.data))
		if len(bd.data) < int(ag.getter.GetBlockSize()) {
			blocks[bd.block] = bd
			stop = true
			break
		}

		blocks[bd.block] = bd

		for {
			bdNext, ok := blocks[nextBlock]
			if !ok {
				break
			}
			d <- nextDownload
			logrus.Infof("Added %v Length: %v", nextDownload, len(bdNext.data))
			nextDownload += 1
			w.Write(bdNext.data)
			delete(blocks, nextBlock)
			nextBlock += 1
		}
	}

	//log.Infof("Done adding blocks. Last added %v", nextDownload)

	for nextBlock < nextDownload {
		//log.Infof("%v %v", nextBlock, nextDownload)
		bdNext, ok := blocks[nextBlock]
		if !ok {
			//log.Infof("Waiting for block %v", nextBlock)
			bd := <-out
			//log.Infof("Got block %v", bd.version)
			blocks[bd.block] = bd
			continue
		}
		w.Write(bdNext.data)
		//log.Infof("Deleting block %v", nextBlock)
		delete(blocks, nextBlock)
		nextBlock += 1
	}

	for i := 0; i < ag.numParallel; i += 1 {
		d <- -1
	}
}
