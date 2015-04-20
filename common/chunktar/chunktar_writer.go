package chunktar

import (
	"archive/tar"
	"bytes"
	"fmt"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/util"
	"io"
	"sync"
	"time"
)

const (
	bufferThreshold = 4 * 1024 * 1024
	bufferLimit     = bufferThreshold * 2
)

type Writer struct {
	tarWriter    *tar.Writer
	inputBuffer  *bytes.Buffer
	outputBuffer *bytes.Buffer
	outputError  error
	open         bool
	wait         *sync.WaitGroup
	name         string
	chunkCount   uint64
}

func NewWriter(out io.Writer) *Writer {
	return &Writer{
		tarWriter:    tar.NewWriter(out),
		inputBuffer:  &bytes.Buffer{},
		outputBuffer: &bytes.Buffer{},
		outputError:  nil,
		open:         true,
		wait:         &sync.WaitGroup{},
	}
}

func (w *Writer) Close() (err error) {
	if !w.open {
		err = fmt.Errorf("chunktar.Writer not open")
		return
	}
	defer func() { w.open = false }()
	err = w.Flush()
	if err != nil {
		return
	}
	err = w.tarWriter.Close()
	return
}

func (w *Writer) Flush() (err error) {
	if !w.open {
		err = fmt.Errorf("chunktar.Writer not open")
		return
	}
	if w.inputBuffer.Len() > 0 {
		err = w.swapInOut()
		if err != nil {
			return
		}
	}
	w.wait.Wait()
	if w.outputError != nil {
		err = w.outputError
		return
	}
	err = w.tarWriter.Flush()
	return
}

func (w *Writer) Write(b []byte) (n int, err error) {
	if !w.open {
		err = fmt.Errorf("chunktar.Writer not open")
		return
	}
	writeSize := len(b)
	for start, stop := 0, util.MinInt(writeSize, bufferLimit-w.inputBuffer.Len()); start < writeSize; start, stop = stop, util.MinInt(stop+bufferLimit, writeSize) {
		if start > 0 {
			err = w.swapInOut()
			if err != nil {
				return
			}
		}
		n, err = w.inputBuffer.Write(b[start:stop])
		if err != nil {
			return
		}
	}
	if w.inputBuffer.Len() > bufferThreshold {
		err = w.swapInOut()
		if err != nil {
			return
		}
	}
	n = writeSize
	return
}

func (w *Writer) WriteHeader(name string) (err error) {
	if !w.open {
		err = fmt.Errorf("chunktar.Writer not open")
		return
	}
	err = w.Flush()
	if err != nil {
		return
	}
	w.name = name
	w.chunkCount = 0
	log.Logf(log.DebugLow, "now writing chunks to %v", w.name)
	return
}

func (w *Writer) swapInOut() (err error) {
	w.wait.Wait()
	if w.outputError != nil {
		err = w.outputError
		return
	}
	// this will get us to about 4 exabytes of chunks without going wider than padding
	chunkName := fmt.Sprintf("%s.%012d", w.name, w.chunkCount)
	w.chunkCount++
	w.inputBuffer, w.outputBuffer = w.outputBuffer, w.inputBuffer
	log.Logf(log.DebugLow, "writing %d byte chunk to %v", w.outputBuffer.Len(), chunkName)
	w.wait.Add(1)
	go w.writeOutput(chunkName)
	w.inputBuffer.Reset()
	return
}

func (w *Writer) writeOutput(chunkName string) {
	defer w.wait.Done()
	header := &tar.Header{
		Name:    chunkName,
		Mode:    0644,
		Size:    int64(w.outputBuffer.Len()),
		ModTime: time.Now(),
	}
	err := w.tarWriter.WriteHeader(header)
	if err != nil {
		w.outputError = err
		return
	}
	_, err = w.outputBuffer.WriteTo(w.tarWriter)
	if err != nil {
		w.outputError = err
		return
	}
	return
}
