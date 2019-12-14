package datamanager

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"io"
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
	//w.Close()
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
	dm        *DataManager
	path      string
	bufChan   chan *bytes.Buffer
	uploadChan chan *UploadInput
	writer    io.WriteCloser
	reader    io.Reader
	lookAhead int
	blockSize int64
}

func (dm *DataManager) NewReverseAggregator(path string, reader io.Reader, lookAhaead int) *ReverseAggregator {
	rag := new(ReverseAggregator)
	rag.path = path

	// TODO: Add uploaders
	rag.lookAhead = lookAhaead
	rag.bufChan = dm.bufChan
	rag.reader, rag.writer = io.Pipe()
	rag.uploadChan = dm.uploadChan
	rag.blockSize = dm.BlockSize
	go rag.ReadFrom(reader)
	return rag
}

func (rag *ReverseAggregator) Read(b []byte) (int, error) {
	return rag.reader.Read(b)
}

func (rag *ReverseAggregator) Close() error {
	return nil
}

func (rag *ReverseAggregator) ReadFrom(reader io.Reader) {
	nextUpload := int64(0)
	out := make(chan bool, rag.lookAhead)

	for {
		out <- true
		buf := bytes.NewBuffer(nil)

		_, readErr := io.CopyN(buf, reader, rag.blockSize)

		if readErr != nil && readErr != io.EOF {
			log.Fatal(readErr)
		}

		_, err := rag.writer.Write(buf.Bytes())

		if err != nil {
			log.Fatal(err)
		}

		rag.uploadChan <- &UploadInput{buf, rag.path,  nextUpload, out}
		nextUpload += 1
		if readErr == io.EOF {
			break
		}
	}

	rag.writer.Close()
}