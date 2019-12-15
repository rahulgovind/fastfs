package fileio

import (
	"errors"
	"github.com/golang-collections/go-datastructures/queue"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Block struct {
	data     []byte
	inMemory bool
	mu       sync.RWMutex
	idx      int64
}

// Safe for concurrent use
// BlockManager handles access to disk blocks using MMIO. Blocks
// are cached in memory until they are written to disk to decrease
// latency
type BlockManager struct {
	mu        sync.RWMutex
	filename  string
	blockSize int64
	numBlocks int64
	io        FileIO
	freeList  *queue.Queue
	nextId    int64
	blockMap  map[int64]*Block
	blockChan chan *TempBlock
}

const (
	MMapInterface = 1
	FileInterface = 2
)

type TempBlock struct {
	idx   int64
	block *Block
}

func NewBlockManager(filename string, numBlocks int64, blockSize int64, iotype int) *BlockManager {
	bm := new(BlockManager)
	bm.blockSize = blockSize
	bm.numBlocks = numBlocks

	if iotype == MMapInterface {
		bm.io = NewMMapIO(filename, blockSize*numBlocks)
	} else if iotype == FileInterface {
		bm.io = NewDirectFileIO(filename, blockSize*numBlocks)
	} else {
		log.Fatal("Invalid IO Type")
	}

	bm.freeList = queue.New(int64(numBlocks))
	bm.nextId = 0
	bm.blockMap = make(map[int64]*Block)
	bm.blockChan = make(chan *TempBlock, 1024)

	for i := 0; i < 16; i += 1 {
		go bm.writer()
	}
	return bm
}

func (bm *BlockManager) writer() {
	for {
		tempblock := <-bm.blockChan
		block := tempblock.block
		idx := tempblock.idx

		offset := idx * bm.blockSize
		startTime := time.Now()

		bm.io.WriteAt(offset, block.data)
		elapsed := time.Since(startTime)
		if false {
			log.Debugf("Copy to block %d complete. Took %v seconds", idx, elapsed)
		}

		block.mu.Lock()
		block.data = nil
		block.inMemory = false
		block.mu.Unlock()
	}
}

// CachePut byte array data. Truncated to blockSize
func (bm *BlockManager) Put(b []byte) (blockId int64, err error) {

	bm.mu.Lock()
	defer bm.mu.Unlock()

	var idx int64
	if bm.freeList.Len() > 0 {
		v, _ := bm.freeList.Get(1)
		idx = v[0].(int64)
	} else {
		if bm.nextId >= bm.numBlocks {
			return -1, errors.New("no more free blocks")
		}
		idx = bm.nextId
		bm.nextId += 1
	}

	block := new(Block)
	block.idx = idx
	block.inMemory = true
	block.data = b

	bm.blockMap[idx] = block
	// Transfer to queue instead

	if len(bm.blockChan) >= cap(bm.blockChan) {
		log.Error("Back pressure from block writer. Slowing down.")
	}

	bm.blockChan <- &TempBlock{idx, block}

	//log.Debug("Created block ", idx)
	return idx, nil
}

// Get block blockId. Truncate to n bytes
func (bm *BlockManager) Get(blockId int64, n int64) ([]byte, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	if block, ok := bm.blockMap[blockId]; ok {
		res := make([]byte, n)
		block.mu.RLock()
		offset := block.idx * bm.blockSize
		if block.inMemory {
			copy(res, block.data)
		} else {
			copy(res, bm.io.ReadAt(offset, n))
		}

		block.mu.RUnlock()
		return res, nil
	}

	return nil, nil
}

// Free block blockId
func (bm *BlockManager) Free(blockId int64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if block, ok := bm.blockMap[blockId]; ok {
		block.mu.Lock()
		defer block.mu.Unlock()
		delete(bm.blockMap, blockId)
		bm.freeList.Put(blockId)
	}
}

func (bm *BlockManager) NumBlocksUsed() int {
	return len(bm.blockMap)
}
