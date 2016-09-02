package clogger

import (
	"bufio"
	"fmt"
	"golib/myio"
	"io"
	"os"
	"strings"
	"sync"
)

type DataLogger struct {
	dirPath string
	file    *os.File
	reader  *bufio.Reader
	lock    *sync.Mutex
	errmsg  string
}

func NewCacheLog(dirPath string, name string) (*DataLogger, error) {
	this := DataLogger{}
	this.dirPath = fmt.Sprintf("%s/%s", dirPath, name)
	this.lock = new(sync.Mutex)
	myio.EnsureDirExists(dirPath)

	var err error
	this.file, err = os.OpenFile(this.dirPath, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	return &this, nil
}

func NewDataLogger(dirPath string, name string) (*DataLogger, error) {
	this := DataLogger{}
	this.dirPath = fmt.Sprintf("%s/%s", dirPath, name)
	this.lock = new(sync.Mutex)
	myio.EnsureDirExists(dirPath)

	var err error
	this.file, err = os.OpenFile(this.dirPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
	return &this, nil
}

func (this *DataLogger) Writelnf(format string, a ...interface{}) {
	this.Write(fmt.Sprintf(format, a...))
}

// Writeln Spaces are not added between operands and a newline is appended.
func (this *DataLogger) Writeln(a ...interface{}) {
	var s string
	for _, name := range a {
		s += fmt.Sprint(name)
	}

	this.Write(s)
}

func (this *DataLogger) Write(line string) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}

	this.file.WriteString(line)
	return nil
}

func (this *DataLogger) Finish() {
	donename := this.dirPath + ".done"
	os.Rename(this.dirPath, donename)
	this.Close()
}

func (this *DataLogger) Close() {
	if this.file != nil {
		this.file.Close()
		this.file = nil
	}
}

func (this *DataLogger) ReadLine() (string, error) {
	if this.file == nil {
		this.errmsg = "file is nil"
		return "", this
	}

	if this.reader == nil {
		this.reader = bufio.NewReader(this.file)
	}

	readString, readerError := this.reader.ReadString('\n')
	if readerError == io.EOF {
		return "", io.EOF
	}

	readString = strings.Replace(readString, "\n", "", -1)
	return readString, nil
}

func (this *DataLogger) Error() string {
	return this.errmsg
}
