package chunktar

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"sync"
)

const (
	bufferThreshold = 4 * 1024 * 1024
	bufferLimit     = bufferThreshold * 2
)

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

type Writer struct {
	tarWriter    *tar.Writer
	inputBuffer  *bytes.Buffer
	outputBuffer *bytes.Buffer
	outputError  error
	open         bool
	wait         *sync.WaitGroup
	name         string
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
	writeSize := len(b)
	for start, stop := 0, min(writeSize, bufferLimit-w.inputBuffer.Len()); start < writeSize; start, stop = stop, min(stop+bufferLimit, writeSize) {
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
	err = w.Flush()
	if err != nil {
		return
	}
	w.name = name
	return
}

func (w *Writer) swapInOut() (err error) {
	w.wait.Wait()
	if w.outputError != nil {
		err = w.outputError
		return
	}
	w.inputBuffer, w.outputBuffer = w.outputBuffer, w.inputBuffer
	w.wait.Add(1)
	go w.writeOutput()
	w.inputBuffer.Reset()
	return
}

func (w *Writer) writeOutput() {
	defer w.wait.Done()
	err := w.tarWriter.WriteHeader(&tar.Header{Name: w.name, Size: int64(w.outputBuffer.Len())})
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
