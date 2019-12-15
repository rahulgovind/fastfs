package metadatamanager

import (
	"errors"
	"fmt"
	"github.com/hashicorp/golang-lru"
	"github.com/rahulgovind/fastfs/common"
	"github.com/rahulgovind/fastfs/s3"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

type MetadataManager struct {
	centralServer *RedisConn
	lru           *lru.Cache
	bucket        string
}

var FileNotFoundError = errors.New("File not found")

func CacheKeyToString(path string, block int64) string {
	return fmt.Sprintf("%v-%v", path, block)
}

func StringToCacheKey(s string) (string, int64) {
	idx := strings.LastIndex(s, "-")
	block, err := strconv.ParseInt(s[idx+1:], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return s[:idx], block
}

func NewMetadataManager(addr string, bucket string, flush bool) *MetadataManager {
	mm := new(MetadataManager)
	mm.centralServer = NewRedisConn(addr)
	if flush {
		mm.centralServer.Flush()
	}
	mm.lru, _ = lru.New(1024 * 128)
	mm.bucket = bucket

	return mm
}

func (mm *MetadataManager) QueryLocation(filepath string, block int64) (string, bool) {
	fLink := CacheKeyToString(filepath, block)
	location, ok := mm.centralServer.Get(fLink)

	if ok {
		return location, true
	}
	return "", false
}

func (mm *MetadataManager) SetLocation(filepath string, block int64, addr string) {
	fLink := CacheKeyToString(filepath, block)
	mm.centralServer.Set(fLink, addr)
}

func (mm *MetadataManager) queryDirect(filepath string) (common.FileInfo, error) {
	for _, node := range s3.ListNodes(mm.bucket, filepath) {
		if node.Path == filepath && !node.IsDirectory {
			return common.FileInfo{node.Path, node.Size}, nil
		}
	}
	return common.FileInfo{}, FileNotFoundError
}

func (mm *MetadataManager) queryServer(filepath string) (common.FileInfo, error) {
	value, ok := mm.centralServer.Get(filepath)
	if ok {
		size, _ := strconv.ParseInt(value, 10, 64)

		return common.FileInfo{filepath, size}, nil
	}
	result, err := mm.queryDirect(filepath)
	if err == FileNotFoundError {
		return result, err
	}

	mm.centralServer.Set(filepath, fmt.Sprintf("%v", result.Size))
	return result, err
}

func (mm *MetadataManager) Query(filepath string) (common.FileInfo, error) {
	value, ok := mm.lru.Get(filepath)
	if ok {
		return value.(common.FileInfo), nil
	}

	// Does the server have it?
	fi, err := mm.queryServer(filepath)

	if err != FileNotFoundError {
		mm.lru.Add(filepath, fi)
	}

	return fi, err
}

func (mm *MetadataManager) AddToList(dir string, filename string, size int64) {
	mm.centralServer.Set(filename, fmt.Sprintf("%d", size))
	mm.centralServer.ListAdd(dir, filename)
}

func (mm *MetadataManager) RemoveFromList(filepath string) {

	if strings.HasSuffix(filepath, "/") {
		// Is a directory
		log.Fatal("Can't delete directorie yet")
	}
	// File
	lastIndex := strings.LastIndex(filepath, "/")
	dir := filepath
	if lastIndex != -1 {
		dir = dir[:lastIndex+1]
	}
	mm.centralServer.ListDelete(dir, filepath)
	mm.centralServer.Delete(filepath)

}

func (mm *MetadataManager) getListDirect(dir string) (common.FileList, error) {
	fmt.Println("getListDirect ", dir)
	var fl common.FileList
	for _, node := range s3.ListNodes(mm.bucket, dir) {
		if !node.IsDirectory {
			fl.Files = append(fl.Files, common.FileInfo{node.Path, node.Size})
		}
	}
	return fl, nil
}

func (mm *MetadataManager) queryListServer(dir string) (common.FileList, error) {
	log.Info("getListServer ", dir)
	value, ok := mm.centralServer.ListGet(dir)
	log.Info("ok: ", ok)
	if ok {
		var result common.FileList
		for _, v := range value {
			fi, err := mm.Query(v)
			if err == nil {
				result.Files = append(result.Files, fi)
			}
		}
		return result, nil
	}

	result, _ := mm.getListDirect(dir)
	var filenames []string
	for _, file := range result.Files {
		filenames = append(filenames, file.Path)
	}

	mm.centralServer.ListAdd(dir, filenames...)

	var keys, values []string
	for _, fi := range result.Files {
		keys = append(keys, fi.Path)
		values = append(values, fmt.Sprintf("%d", fi.Size))
	}

	mm.centralServer.MSet(keys, values)

	return result, nil
}

func (mm *MetadataManager) GetList(dir string) (common.FileList, error) {
	// Not caching locally here since directories can be updated by others
	return mm.queryListServer(dir)
}
