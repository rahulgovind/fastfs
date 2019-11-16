// Metadata Manager
package metadatamanager

import (
	"bytes"
	"errors"
	"github.com/rahulgovind/fastfs/s3"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const BlockSize int64 = 16 * 1024 * 1024

// Path: block b at file/base/blocks[block]
type CompactedFile struct {
	fileSize  int64
	blockSize int64
}

func (c *CompactedFile) MaxBlocks() int64 {
	return (c.fileSize + c.blockSize - 1) / c.blockSize
}

func NewCompactedFile(fileSize int64) *CompactedFile {
	cf := new(CompactedFile)
	cf.fileSize = fileSize
	cf.blockSize = BlockSize
	return cf
}

// Path: file/version/block
type VersionedFile struct {
	fileSize  int64
	blockSize int64
	blocks    map[int64]bool
	version   int64
}

func (v *VersionedFile) MaxBlocks() int64 {
	return (v.fileSize + v.blockSize - 1) / v.blockSize
}

func NewVersionedFile(fileSize int64) *VersionedFile {
	vf := new(VersionedFile)
	vf.blocks = make(map[int64]bool)
	vf.version = -1

	return vf
}

// Each file is a list of VersionedFiles
type VersionedFileList struct {
	mu sync.RWMutex
	l  []*VersionedFile
}

// Path: file/scratch/random
type ScratchFile struct {
	fileSize  int64
	blockSize int64
	blocks    map[int64]string
}

func (s *ScratchFile) MaxBlocks() int64 {
	return (s.fileSize + s.blockSize - 1) / s.blockSize
}

func (vfl *VersionedFileList) Lookup(version int64, block int64) int64 {
	vfl.mu.RLock()
	defer vfl.mu.RUnlock()

	if len(vfl.l) == 0 || version < vfl.l[0].version {
		return -1
	}

	i := sort.Search(len(vfl.l), func(i int) bool { return vfl.l[i].version >= version })

	if vfl.l[i].version != version {
		log.Infof("Missing version %v. Found %v", version, vfl.l[i].version)
	}

	for ; i >= 0; i-- {
		if block >= vfl.l[i].MaxBlocks() {
			return -1
		}
		if _, ok := vfl.l[i].blocks[block]; ok {
			return vfl.l[i].version
		}
	}

	return -1
}

type FileData struct {
	Base  *CompactedFile
	Delta *VersionedFileList
}

func (fd *FileData) GetLatestVersion() int64 {
	if fd.Delta == nil || len(fd.Delta.l) == 0 {
		return 0
	}
	return fd.Delta.l[len(fd.Delta.l)-1].version
}

type FileMap struct {
	mu sync.RWMutex
	m  map[string]FileData
}

type WorkFile struct {
	mu sync.RWMutex
	fm *FileMap
	s  ScratchFile
}

func NewFileMap() *FileMap {
	fm := new(FileMap)
	fm.m = make(map[string]FileData)
	return fm
}

type FileIdentifier struct {
	Path    string
	Version int64
}

// File Descriptor Table
type FDTable struct {
	// File version
	mu      sync.RWMutex
	lastVal int64
	m       map[int64]FileIdentifier // Map FD to file and version
	minv    map[FileIdentifier]int64 // Inverse map
}

func NewFDTable() *FDTable {
	fdt := new(FDTable)
	fdt.m = make(map[int64]FileIdentifier)
	fdt.minv = make(map[FileIdentifier]int64)
	fdt.lastVal = 0
	return fdt
}

func (fdt *FDTable) Open(path string, version int64) int64 {
	fdt.mu.Lock()
	defer fdt.mu.Unlock()

	newFd := fdt.lastVal
	fdt.lastVal += 1

	fid := FileIdentifier{path, version}
	fdt.m[newFd] = fid
	v, ok := fdt.minv[fid]
	if !ok {
		fdt.minv[fid] = 1
	} else {
		fdt.minv[fid] = v + 1
	}

	return newFd
}

func (fdt *FDTable) Close(fd int64) bool {
	fdt.mu.Lock()
	defer fdt.mu.Unlock()

	if fid, ok := fdt.m[fd]; ok {
		delete(fdt.m, fd)
		v := fdt.minv[fid]
		v -= 1
		if v == 0 {
			delete(fdt.minv, fid)
		} else {
			fdt.minv[fid] = v
		}
		return true
	}
	return false
}

func (fdt *FDTable) Lookup(fd int64) (FileIdentifier, bool) {
	fdt.mu.RLock()
	defer fdt.mu.RUnlock()
	fid, ok := fdt.m[fd]
	return fid, ok
}

func (fdt *FDTable) PrintOpenFiles() {
	fdt.mu.RLock()
	defer fdt.mu.RUnlock()

	for fd, fid := range fdt.m {
		log.Infof("FD: %v\tOpen File: %v\tVersion: %v", fd, fid.Path, fid.Version)
	}
}

type MetadataManager struct {
	mu     sync.RWMutex
	fm     *FileMap
	bucket string
	smm    *s3.MetadataManager
	om     *FDTable
	fdt    *FDTable
}

func NewMetadataManager(bucket string) *MetadataManager {
	result := new(MetadataManager)
	result.bucket = bucket
	result.fm = NewFileMap()
	result.smm = s3.NewS3MetadataManager(bucket)
	result.fdt = NewFDTable()
	return result
}

func SplitPath(path string) []string {
	path = strings.TrimSuffix(path, "/")
	result := strings.Split(path, "/")
	if len(result) > 0 {
		result = result[1:]
	}
	return result
}

func BufferToInt(buf *bytes.Buffer) (int64, error) {
	var i int64

	re := regexp.MustCompile("^([0-9]*)")
	b, _ := ioutil.ReadAll(buf)

	s := string(b)
	match := re.FindStringSubmatch(s)
	if len(match) == 0 {
		log.Error("Invalid file size data")
		return -1, errors.New("invalid file size data")
	}

	i, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		log.Error(err)
		return -1, err
	}
	return i, err
}

func (mm *MetadataManager) loadFileSize(path string, suffix string) (int64, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	err := s3.DownloadToWriter(mm.bucket,
		strings.TrimPrefix(filepath.Join(path, suffix, "_size"), "/"), buf)
	if err != nil {
		log.Error(err)
		return -1, err
	}

	return BufferToInt(buf)
}

func (mm *MetadataManager) loadBlocks(path string, suffix string, version int64) (*VersionedFile, error) {
	fileSize, err := mm.loadFileSize(path, suffix)
	vf := NewVersionedFile(fileSize)
	if err != nil {
		return nil, err
	}

	re := regexp.MustCompile("^[0-9]*$")

	for _, inode := range mm.smm.Stat(path) {
		match := re.FindStringSubmatch(inode.Key)
		if len(match) == 0 {
			continue
		}
		var blockNumber int64
		blockNumber, err = strconv.ParseInt(filepath.Join(path, inode.Key), 10, 64)
		if err != nil {
			log.Error(err)
		}
		vf.blocks[blockNumber] = true
	}
	return vf, nil
}

// LoadFileMetadata loads all the metadata we need to use FastFS
func (mm *MetadataManager) LoadFileMetadata(filename string, path string) error {
	re := regexp.MustCompile("^_(base|scratch|[0-9]*)$")
	var result []string

	var cf *CompactedFile = nil
	vfl := new(VersionedFileList)

	for _, inode := range mm.smm.Stat(path) {
		path2 := filepath.Join(path, inode.Key)

		match := re.FindStringSubmatch(inode.Key)

		if len(match) == 0 {
			result = append(result, path2)
			log.Infof("External file found: %v", path2)
			continue
		}

		if match[1] == "base" {
			log.Infof("Loading base for %v", path)
			fileSize, err := mm.loadFileSize(path, inode.Key)
			if err != nil {
				log.Error(err)
				return err
			}
			cf = NewCompactedFile(fileSize)
			log.Infof("Base file loaded for %v. File Size: %v", filename, fileSize)
		} else if match[1] == "scratch" {
			// TODO: Delete files here
		} else { // match[1] == "1234"
			version, err := strconv.ParseInt(match[1], 10, 64)
			if err != nil {
				log.Errorf("Failed ot parse %v to int", match[1])
				return err
			}
			vf, err := mm.loadBlocks(path, inode.Key, version)
			vfl.l = append(vfl.l, vf)
		}
	}

	sort.Slice(vfl.l, func(i, j int) bool {
		return vfl.l[i].version < vfl.l[j].version
	})

	if cf == nil {
		return nil
	}

	mm.fm.m[filename] = FileData{cf, vfl}

	return nil
}

// Currently only one layer
func (mm *MetadataManager) LoadFS() error {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	log.Infof("LoadFS")
	re := regexp.MustCompile("^(.*)__fastfs$")

	for _, inode := range mm.smm.Stat("") {
		log.Infof("Loading metadata for %v", inode.Key)
		match := re.FindStringSubmatch(inode.Key)
		if len(match) == 0 {
			continue
		}

		mm.LoadFileMetadata(inode.Key, "/"+inode.Key)
	}

	log.Infof("All files: ")
	for k, _ := range mm.fm.m {
		log.Info(k)
	}
	return nil
}

func (mm *MetadataManager) Open(path string) (int64, error) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	mm.fm.mu.RLock()
	defer mm.fm.mu.RUnlock()

	if fd, ok := mm.fm.m[path]; ok {
		latestVersion := fd.GetLatestVersion()
		log.Infof("Latest version for %v: %v", path, latestVersion)
		result := mm.fdt.Open(path, latestVersion)
		mm.fdt.PrintOpenFiles()
		return result, nil
	} else {
		log.Error("Attempted to open non-existent file: %v", path)
	}
	return -1, nil
}

func (mm *MetadataManager) GetFiles() []string {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	var result []string

	for file, _ := range mm.fm.m {
		result = append(result, file)
	}
	return result
}
