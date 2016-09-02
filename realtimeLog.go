package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"compress/gzip"

	applogs "./logappdirsignquery"
	calogs "./logcachingpackagesignquery"
	mytools "./tools"
)

var process_quit chan int

const (
	proc = 1 << iota //process
	task             //fillTask_syslog
	taskwalk
)

var task_ch chan string

var dataSourceFile string
var logType string

func init() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: datasource [logtype: appdirsignquery|cachingpackagesignquery]")
		os.Exit(1)
	}

	for i := 0; i < len(os.Args) && i < 4; i++ {
		switch i {
		case 1:
			dataSourceFile = os.Args[i]
		case 2:
			logType = os.Args[i]
		case 3:
			mytools.Datestr = os.Args[i]
		}
	}

	fmt.Println("NumCPU:", runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())

	task_ch = make(chan string, 3000000)
	process_quit = make(chan int)
}

func filltaskWalk(path string, info os.FileInfo, err error) error {
	defer errprocess(taskwalk)

	//TODO
	if err != nil || info.IsDir() || !strings.HasSuffix(path, ".gz.done") {
		return nil
	}

	inputFile, inputError := os.OpenFile(path, os.O_RDONLY|os.O_SYNC, 0666)
	if inputError != nil {
		return inputError
	}

	gzipreader, ReaderErr := gzip.NewReader(inputFile)
	if ReaderErr != nil {
		fmt.Fprintln(os.Stderr, time.Now(), "gzip.NewReader", ReaderErr, path)
		return ReaderErr
	}

	//inputReader := bufio.NewReader(inputFile)

	var inputString string
	for {
		p := make([]byte, 1024)
		n, _ := gzipreader.Read(p)
		if n == 0 {
			if len(inputString) > 0 {
				task_ch <- inputString
			}
			fmt.Fprintln(os.Stdout, time.Now(), "read file finished", path)
			break
		}
		inputString += string(p)
		vec := strings.Split(inputString, "\n")
		cap := len(vec)
		for i := 0; i < (cap - 1); i++ {
			task_ch <- vec[i]
		}
		inputString = vec[cap-1]
	}
	gzipreader.Close()
	inputFile.Close()

	return nil
}

func mywalkfunc(path string, info os.FileInfo, err error) error {
	go filltaskWalk(path, info, err)
	return nil
}

func fillTask_syslog() {
	defer errprocess(task)

	for {
		inputFile, inputError := os.OpenFile(dataSourceFile, os.O_RDONLY|os.O_SYNC, 0666)
		if inputError != nil {
			panic(inputError)
		}
		// Luanch only one instance.
		//if err := syscall.Flock(int(inputFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		//	fmt.Fprintln(os.Stderr, time.Now(), "Error - %s flock failed [%s]", dataSourceFile, err)
		//	os.Exit(1)
		//}

		inputReader := bufio.NewReader(inputFile)
		for {
			inputString, readerError := inputReader.ReadString('\n')
			if readerError == io.EOF {
				fmt.Fprintln(os.Stderr, time.Now(), "io.EOF: ", io.EOF, inputString)
				time.Sleep(1 * time.Second)
				break
			}
			task_ch <- inputString
		}
		inputFile.Close()
	}
}

func main() {

	fmt.Fprint(os.Stdout, time.Now(), "started", os.Args)

	fileInfo, err := os.Lstat(dataSourceFile)
	if err != nil {
		fmt.Fprint(os.Stdout, time.Now(), "os.Lstat ", dataSourceFile, err)
		os.Exit(1)
	}

	if fileInfo.IsDir() {
		go filepath.Walk(dataSourceFile, mywalkfunc)
		go normalExit(0)
	} else if fileInfo.Mode()&os.ModeNamedPipe != 0 {
		go fillTask_syslog()
	}

	if logType == "appdirsignquery" {
		applogs.DonotAutoinit()
	} else if logType == "cachingpackagesignquery" {
		calogs.DonotAutoinit()
	} else {
		applogs.DonotAutoinit()
		calogs.DonotAutoinit()
	}

	num := runtime.NumCPU() * 2
	for i := 0; i < num; i++ {
		go process()
	}
	process_wait()
}

func process_wait() {
	for {
		time.Sleep(1 * time.Second)
		switch <-process_quit {
		case proc:
			go process()
			fmt.Fprintln(os.Stderr, time.Now(), "restart process")
		case task:
			go fillTask_syslog()
			fmt.Fprintln(os.Stderr, time.Now(), "restart fillTask_syslog")
		case taskwalk:
			fmt.Fprintln(os.Stdout, time.Now(), "pathwalk finished")
		default:
			fmt.Fprintln(os.Stderr, time.Now(), "wrong quit sign")
		}
	}
}

func errprocess(which int) {
	if err := recover(); err != nil {
		fmt.Fprintln(os.Stderr, time.Now(), err, ";trace", string(debug.Stack()))
		process_quit <- which
	}
}

func process() {
	defer errprocess(proc)

	for {
		str_log, ok := <-task_ch
		if ok == false {
			fmt.Println("task_ch closed!")
			os.Exit(1) //  This never happens
		}

		// use syslog assert contains "appdirsignquery" or "cachingpackagesignquery" and quit when read line == "quit"
		// path walk will have logType
		str_log = strings.Replace(str_log, "\n", "", 1)
		if strings.Contains(str_log, "appdirsignquery") || logType == "appdirsignquery" {
			applogs.AppDirSignQueryCount.Record(str_log)
		} else if strings.Contains(str_log, "cachingpackagesignquery") || logType == "cachingpackagesignquery" {
			calogs.CachingpackagesignqueryCount.Record(str_log)
		} else if str_log == "quit" {
			fmt.Fprint(os.Stdout, time.Now(), "finished")
			normalExit(0)
		} else {
			fmt.Fprintf(os.Stderr, "wrong log: {%s}", str_log)
		}
	}
}

func normalExit(stat int) {
	time.Sleep(3 * time.Second)
	for len(task_ch) != 0 {
		fmt.Println(time.Now(), "task_ch len = ", len(task_ch))
		time.Sleep(10 * time.Second)
	}

	// path walk
	if logType == "appdirsignquery" {
		applogs.AppDirSignQueryCount.Out()
	} else if logType == "cachingpackagesignquery" {
		calogs.CachingpackagesignqueryCount.Out()
	} else {
	}

	time.Sleep(3 * time.Second)
	os.Exit(stat)
}
