package chunktar

import (
	//	"archive/tar"
	//	"bytes"
	"fmt"
	//	"github.com/mongodb/mongo-tools/common/log"
	//	"github.com/mongodb/mongo-tools/common/util"
	"io"
	"os"
	//	"sync"
	//	"time"
)

type Reader struct {
	sourceFile *os.File
	source     io.Reader
}

func NewReader(path string) (*Reader, error) {
	if path == "-" {
		return &Reader{source: os.Stdin}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("unable to open tar archive file `%v` for reading: %v", path, err)
	}
	return &Reader{
		sourceFile: file,
		source:     file,
	}, nil
}
