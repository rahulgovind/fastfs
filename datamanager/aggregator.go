package datamanager

import (
	log "github.com/sirupsen/logrus"
	"io"
	"sync"
	"time"
)

type BlockData struct {
	data  []byte
	block int64
}

type Aggregator struct {
	path        string
	numParallel int
	start       int64
	end         int64
	getter      BlockGetter
}

func NewAggregator(numParallel int, path string,
	start int64, end int64,
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

func (ag *Aggregator) downloadBlock(blockCh chan int64, out chan *BlockData) {
	for {
		block := <-blockCh

		//log.Debugf("look-ahead %v", block)
		if block == -1 {
			break
		}

		data, err := ag.getter.Get(ag.path, block)
		if err != nil {
			log.Fatal(err)
		}

		//log.Infof("Got data from Get. Block: %v, Length: %v", block, len(data))
		out <- &BlockData{data, block}
	}
}

func (ag *Aggregator) WriteTo(w io.WriteCloser) {

	blockSize := ag.getter.GetBlockSize()
	startBlock := ag.start / ag.getter.GetBlockSize()
	endBlock := ag.end / blockSize

	startOff := ag.start % blockSize
	endOff := ag.end%blockSize + 1

	nextBlock := startBlock
	nextDownload := startBlock
	stop := false
	blocks := make(map[int64]*BlockData)

	d := make(chan int64, ag.numParallel)
	out := make(chan *BlockData, ag.numParallel)

	for i := 0; i < ag.numParallel; i++ {
		d <- nextDownload
		nextDownload += 1
		//log.Infof("Added %v", nextDownload-1)
		go ag.downloadBlock(d, out)
	}

	for !stop {
		if nextDownload > endBlock {
			break
		}

		bd := <-out
		//log.Infof("Got block %v. Length: %v ", bd.block, len(bd.data))

		if len(bd.data) < int(ag.getter.GetBlockSize()) {
			blocks[bd.block] = bd
			stop = true
			break
		}

		blocks[bd.block] = bd

		for {

			//log.Infof("Waiting for block %v", nextBlock)
			bdNext, ok := blocks[nextBlock]
			if !ok {
				break
			}

			if nextDownload <= endBlock {
				d <- nextDownload
				nextDownload += 1
			}

			//log.Infof("Added %v Length: %v", nextDownload, len(bdNext.data))

			startOffHere := int64(0)
			endOffHere := int64(len(bdNext.data))

			if bdNext.block == startBlock {
				startOffHere = startOff
			}

			if bdNext.block == endBlock && endOff < endOffHere {
				endOffHere = endOff
			}

			start := time.Now()
			if endOffHere-startOffHere > 0 {
				//log.Debugf("Writing block %v", bdNext.block)
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
			//log.Infof("Waiting for block %v", nextBlock)
			bd := <-out
			//log.Infof("Got block %v", bd.version)
			blocks[bd.block] = bd
			continue
		}
		startOffHere := int64(0)
		endOffHere := int64(len(bdNext.data))

		if bdNext.block == startBlock {
			startOffHere = startOff
		}

		if bdNext.block == endBlock && endOff < endOffHere {
			endOffHere = endOff
		}

		start := time.Now()
		if endOffHere-startOffHere > 0 {
			//log.Debugf("Writing block %v", bdNext.block)
			w.Write(bdNext.data[startOffHere:endOffHere])
		}
		elapsed := time.Since(start)

		log.Infof("Writing block took %v", elapsed)
		delete(blocks, nextBlock)
		nextBlock += 1
	}

	for i := 0; i < ag.numParallel; i += 1 {
		d <- -1
	}
	w.Close()
}

type FakeWriteCloser struct {
	Writer io.Writer
}

func (f *FakeWriteCloser) Write(b []byte) (int, error) {
	return f.Writer.Write(b)
}

func (f *FakeWriteCloser) Close() error {
	return nil
}

type ReverseAggregator struct {
	path        string
	putter      BlockPutter
	writer      io.WriteCloser
	numParallel int
}

func NewReverseAggregator(path string, putter BlockPutter, writer io.WriteCloser, numParallel int) *ReverseAggregator {
	rag := new(ReverseAggregator)
	rag.path = path
	rag.putter = putter
	rag.writer = writer
	rag.numParallel = numParallel

	return rag
}

func (rag *ReverseAggregator) uploadBlock(blockCh chan *BlockData, bufChan chan []byte) {
	for {
		blockData := <-blockCh

		//log.Debugf("look-ahead %v", block)
		if blockData == nil {
			break
		}

		log.Debug("Caching block ", blockData.block)
		err := rag.putter.Put(rag.path, blockData.block, blockData.data)
		if err != nil {
			log.Fatal(err)
		}

		bufChan <- blockData.data
	}
}

type SharedBuffer struct {
	mu   sync.Mutex
	data [][]byte
}

func (rag *ReverseAggregator) ReadFrom(reader io.ReadCloser) {
	log.Debug("Starting read from")
	blockSize := rag.putter.GetBlockSize()

	blockChan := make(chan *BlockData, rag.numParallel)

	nextUpload := int64(0)
	bufChan := make(chan []byte, rag.numParallel)

	for i := 0; i < rag.numParallel; i++ {
		bufChan <- make([]byte, blockSize)
		go rag.uploadBlock(blockChan, bufChan)
	}

	for {
		buf := <-bufChan
		buf = buf[:cap(buf)]

		log.Debug("Reading block ", nextUpload)

		n, readErr := io.ReadAtLeast(reader, buf, len(buf))
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			log.Fatal(readErr)
		}

		log.Debug("Done reading. Writing now.", nextUpload)
		_, err := rag.writer.Write(buf[:n])
		if err != nil {
			log.Fatal(err)
		}

		blockChan <- &BlockData{buf[:n], nextUpload}
		nextUpload += 1
		if readErr == io.EOF {
			break
		}
	}

	log.Info("Done")
	for i := 0; i < rag.numParallel; i++ {
		blockChan <- nil
	}

	log.Info("Exit")
	reader.Close()
}
