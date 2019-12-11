package datamanager

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"time"
)

type BlockData struct {
	data  []byte
	block int
}

type Aggregator struct {
	path        string
	numParallel int
	start       int
	end         int
	getter      BlockGetter
}

func NewAggregator(numParallel int, path string,
	start int, end int,
	getter BlockGetter) *Aggregator {
	res := new(Aggregator)
	res.getter = getter
	res.path = path
	res.numParallel = numParallel
	res.start = start
	res.end = end
	if end == -1 {
		res.end = 1024 * 1024 * 1024
	}
	return res
}

func (ag *Aggregator) downloadBlock(blockCh chan int, out chan *BlockData) {
	for {
		block := <-blockCh

		log.Debugf("look-ahead %v", block)
		if block == -1 {
			break
		}

		data, err := ag.getter.Get(ag.path, block)
		if err != nil {
			log.Fatal(err)
		}

		log.Infof("Got data from Get. Block: %v, Length: %v", block, len(data))
		out <- &BlockData{data, block}
	}
}

func (ag *Aggregator) WriteTo(w io.Writer) {

	blockSize := ag.getter.GetBlockSize()
	startBlock := ag.start / ag.getter.GetBlockSize()
	endBlock := ag.end / blockSize

	startOff := ag.start % blockSize
	endOff := ag.end%blockSize + 1

	nextBlock := startBlock
	nextDownload := startBlock
	stop := false
	blocks := make(map[int]*BlockData)

	d := make(chan int, ag.numParallel)
	out := make(chan *BlockData, ag.numParallel)

	for i := 0; i < ag.numParallel; i++ {
		d <- nextDownload
		nextDownload += 1
		log.Infof("Added %v", nextDownload-1)
		go ag.downloadBlock(d, out)
	}

	for !stop {
		if nextDownload > endBlock {
			break
		}

		bd := <-out
		log.Infof("Got block %v. Length: %v ", bd.block, len(bd.data))

		if len(bd.data) < int(ag.getter.GetBlockSize()) {
			blocks[bd.block] = bd
			stop = true
			break
		}

		blocks[bd.block] = bd

		for {

			log.Infof("Waiting for block %v", nextBlock)
			bdNext, ok := blocks[nextBlock]
			if !ok {
				break
			}

			if nextDownload <= endBlock {
				d <- nextDownload
				nextDownload += 1
			}

			log.Infof("Added %v Length: %v", nextDownload, len(bdNext.data))

			startOffHere := 0
			endOffHere := len(bdNext.data)

			if bdNext.block == startBlock {
				startOffHere = startOff
			}

			if bdNext.block == endBlock && endOff < endOffHere {
				endOffHere = endOff
			}

			start := time.Now()
			if endOffHere-startOffHere > 0 {
				log.Debugf("Writing block %v", bdNext.block)
				w.Write(bdNext.data[startOffHere:endOffHere])
			}
			elapsed := time.Since(start)

			log.Infof("Writing block took %v", elapsed)

			nextBlock += 1
			if nextDownload > endBlock {
				break
			}
		}
	}

	//log.Infof("Done adding blocks. Last added %v", nextDownload)

	for nextBlock < nextDownload {
		//log.Infof("%v %v", nextBlock, nextDownload)
		bdNext, ok := blocks[nextBlock]
		if !ok {
			log.Infof("Waiting for block %v", nextBlock)
			bd := <-out
			//log.Infof("Got block %v", bd.version)
			blocks[bd.block] = bd
			continue
		}
		startOffHere := 0
		endOffHere := len(bdNext.data)

		if bdNext.block == startBlock {
			startOffHere = startOff
		}

		if bdNext.block == endBlock && endOff < endOffHere {
			endOffHere = endOff
		}

		start := time.Now()
		if endOffHere-startOffHere > 0 {
			log.Debugf("Writing block %v", bdNext.block)
			w.Write(bdNext.data[startOffHere:endOffHere])
		}
		elapsed := time.Since(start)

		log.Infof("Writing block took %v", elapsed)
		delete(blocks, nextBlock)
		nextBlock += 1
	}

	fmt.Println("Done")
	for i := 0; i < ag.numParallel; i += 1 {
		d <- -1
	}
}
