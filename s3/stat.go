// Metadata Manager
package s3

import (
	"fmt"
	"strings"
	"sync"
)

type INode struct {
	mu       *sync.RWMutex
	Children map[string]*INode
	Key      string
	IsDir    bool
	isLoaded bool   // only valid if directory
	fLink    string // only valid if file
	FileSize int64
}

func NewINode(key string, isDir bool, fileSize int64, fLink string) *INode {
	n := new(INode)
	n.Key = key
	n.isLoaded = !isDir
	n.IsDir = isDir
	n.FileSize = fileSize
	n.fLink = fLink
	n.Children = make(map[string]*INode)
	return n
}

func (inode *INode) insert(n *INode) bool {
	if _, ok := inode.Children[n.Key]; !ok {
		inode.Children[n.Key] = n
	}
	return false
}

type MetadataManager struct {
	mu   sync.Mutex
	rt   *INode
	load func(string) []*INode
}

func (mm *MetadataManager) GetRoot() *INode {
	return mm.rt
}

func (mm *MetadataManager) loadAt(inode *INode) {
	if inode != nil && inode.IsDir && !inode.isLoaded {
		inodes := mm.load(inode.fLink)
		for _, inode2 := range inodes {
			inode.insert(inode2)
		}
		inode.isLoaded = true
	}
}

func (mm *MetadataManager) Stat(path string) []*INode {
	path = strings.TrimSuffix(path, "/")
	keys := strings.Split(path, "/")
	keys = keys[1:]

	node := mm.rt
	mm.loadAt(node)

	for _, key := range keys {
		node2, ok := node.Children[key]
		if !ok || !node2.IsDir {
			return nil
		}
		node = node2
		mm.loadAt(node2)
	}

	if node == nil {
		return nil
	}

	var result []*INode
	for _, inode := range node.Children {
		result = append(result, inode)
	}
	return result
}

func (mm *MetadataManager) printTree(path string) {
	fmt.Println(path)
	for _, c := range mm.Stat(path) {

		if c.IsDir {
			mm.printTree(path + "/" + c.Key)
		} else {
			fmt.Println(path+"/"+c.Key, "\t", c.FileSize)
		}
	}
}

// Utility function to print tree of files
func (mm *MetadataManager) PrintTree() {
	for _, c := range mm.Stat("") {
		mm.printTree("/" + c.Key)
	}
}

func lastString(ss []string) string {
	return ss[len(ss)-1]
}

// Query data for a single file
func (mm *MetadataManager) Query(path string) *INode {
	nodes := mm.load(path)
	if len(nodes) == 0 || len(nodes) > 1 { // Not a file
		return nil
	}
	return nodes[0]
}

func NewS3MetadataManager(bucket string) *MetadataManager {
	mm := new(MetadataManager)
	mm.rt = NewINode("", true, 0, "")
	mm.load = func(path string) []*INode {
		temp := ListNodes(bucket, path)
		var result []*INode
		for _, node := range temp {
			node.Path = strings.TrimSuffix(node.Path, "/")
			result = append(result,
				NewINode(lastString(strings.Split(node.Path, "/")),
					node.IsDirectory, node.Size, node.Path+"/"))
		}
		return result
	}

	return mm
}
