package common

type FileList struct {
	Files []FileInfo
}

type FileInfo struct {
	Path string
	Size int64
}

