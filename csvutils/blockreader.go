package csvutils

import "bytes"

func AlignedSlices(raw []byte, startSkip bool, blockSize int) []byte {
	startOffset := 0

	if startSkip {
		startOffset := bytes.LastIndex(raw[:blockSize], []byte{0x0a})
		if startOffset == -1 {
			return []byte{}
		}
	}
	skipOffset := startOffset
	if len(raw)-blockSize > skipOffset {
		skipOffset = len(raw) - blockSize
	}

	lastIndex := bytes.LastIndex(raw[skipOffset:], []byte{0x0a})
	if lastIndex == -1 {
		return []byte{}
	}
	return raw[startOffset : skipOffset+lastIndex]
}

//
//func (br *BlockReader) Read(p []byte) (int, error) {
//	// First get done with the part we need to skip
//	var err error
//	for br.startSkip {
//		var n int
//		n, err = br.r.Read(br.buf1.Bytes()[br.totalWritten:])
//		if err != nil && err != io.EOF {
//			return 0, err
//		}
//
//		toRead := br.blockSize - br.totalWritten
//		if toRead > n {
//			toRead = n
//		}
//		br.totalWritten += toRead
//
//		if br.totalWritten == br.blockSize {
//			lastNewLine := bytes.LastIndex(br.buf1.Bytes(), []byte{0x0A})
//			if lastNewLine != 0 {
//				br.buf2.Write(br.buf1.Bytes()[lastNewLine+1:])
//			}
//			br.buf1.Reset()
//			br.buf1, br.buf2 = br.buf2, br.buf1
//			br.startSkip = true
//			br.totalWritten = 0
//		} else if err == io.EOF {
//			return 0, io.EOF
//		}
//	}
//
//	alreadyWritten := copy(p[:], br.buf1.Bytes()[br.totalRead:])
//	br.totalRead += alreadyWritten
//	if alreadyWritten == len(p) || err == io.EOF {
//		return alreadyWritten, err
//	}
//
//	for alreadyWritten < len(p) {
//
//	}
//}
