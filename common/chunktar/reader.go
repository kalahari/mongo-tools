package chunktar

import (
	"archive/tar"
	"fmt"
	"github.com/mongodb/mongo-tools/common/log"
	"io"
	"regexp"
	"strconv"
)

var chunkFileNameRegex = regexp.MustCompile(`^(.+)\.(\d{12})$`)

type Reader struct {
	tarReader *tar.Reader
	name      string
	number    uint64
	nextName  string
}

func NewReader(source io.Reader) *Reader {
	return &Reader{tarReader: tar.NewReader(source)}
}

func (r *Reader) Next() (string, error) {
	for err := error(nil); r.nextName == ""; err = r.nextChunk() {
		if err != nil {
			return "", err
		}
	}
	r.name = r.nextName
	r.nextName = ""
	r.number = 0
	return r.name, nil
}

func (r *Reader) Read(b []byte) (int, error) {
	if r.nextName != "" || r.name == "" {
		return 0, io.EOF
	}
	if len(b) == 0 {
		return 0, nil
	}
	n, err := r.tarReader.Read(b)
	if err == io.EOF {
		err = r.nextChunk()
		if err != nil {
			return n, err
		}
		if n == 0 && r.nextName == "" {
			n, err = r.tarReader.Read(b)
		}
	}
	return n, err
}

func (r *Reader) nextChunk() error {
	header, err := r.tarReader.Next()
	if err == io.EOF {
		return err
	} else if err != nil {
		return fmt.Errorf("unable to read next file from tar archive: %v", err)
	}
	log.Logf(log.DebugLow, "read tar header for `%v`, %v bytes", header.Name, header.Size)
	if match := chunkFileNameRegex.FindStringSubmatch(header.Name); match != nil {
		name := match[1]
		chunk, err := strconv.ParseUint(match[2], 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse chunk number from file `%v`: %v", header.Name, err)
		}
		if name == r.name {
			if chunk != r.number+1 {
				return fmt.Errorf("chunks out of order for `%v`, expected %v but got %v", name, r.number+1, chunk)
			}
			r.number = chunk
			return nil
		}
		if chunk != 0 {
			return fmt.Errorf("missing first chunk for `%v`, expected 0 but got %v", name, chunk)
		}
		r.nextName = name
		r.name = ""
		return nil
	}
	// tar file not chunked
	r.nextName = header.Name
	r.name = ""
	return nil
}
