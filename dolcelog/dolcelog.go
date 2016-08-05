// Package dolcelog provides an append only log and functions to handle transactions with it.
package dolcelog

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrNotFound = errors.New("Log file not found or created.")
)

/*
DolceLog is the basic structure used by the log
- filename is the log filename
- path is the folder path of the log
- file is a point to the log file
- index is the last number used as an index in the log
- logMutex is a mutex to lock/unlock writing to the log
*/
type DolceLog struct {
	filename string
	path     string
	file     *os.File
	version  int
	index    uint64
	logMutex sync.Mutex
}

// New creates and initializes a new DolceLog
func New(fp, fn string) (*DolceLog, error) {
	dlog := &DolceLog{
		version:  1,
		filename: fn,
		path:     fp,
		index:    0, // TODO change this to reflect the index of the log when the server is restarted
	}

	var filepath = dlog.path + "/" + dlog.filename

	//Check if file exists
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		// If not create it
		f, err := os.Create(filepath)
		if err != nil {
			log.Fatal(err)
		}

		dlog.file = f
		return nil, ErrNotFound
	}

	// If file exists open it
	f, err := os.OpenFile(filepath, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, ErrNotFound
	}

	fmt.Println("Log file found.")
	dlog.file = f

	return dlog, nil
}

// Set appened to the log
func (l *DolceLog) Set(key string, value []byte) {
	l.logMutex.Lock()
	defer l.logMutex.Unlock()

	wr := bufio.NewWriter(l.file)
	_, err := fmt.Fprintf(wr, "%d  S %s %s\n", l.index, key, value)
	if err != nil {
		fmt.Println(err)
	}

	l.index++

	err = wr.Flush()
	if err != nil {
		log.Fatal("Data not writen in file.")
	}
}

// GetFromIndex returns the log after a specific index.
// TODO implement a better file parser
func (l *DolceLog) GetFromIndex(index int) ([]string, error) {
	l.logMutex.Lock()
	defer l.logMutex.Unlock()

	var result = make([]string, 0)
	i := 0

	f, err := os.Open(l.filename)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		sepIndex := strings.Index(line, " ")
		id, err := strconv.Atoi(line[:sepIndex])
		if err != nil {
			log.Fatal("Index retrieval error.")
		}

		if index <= id {
			result = append(result, line)
		}
		i++
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return result, nil
}

// GetAll returns a slice of the with the whole log.
func (l *DolceLog) GetAll() ([]string, error) {
	l.logMutex.Lock()
	defer l.logMutex.Unlock()
	var i uint64
	var result = make([]string, 0)
	i = 0

	f, err := os.Open(l.filename)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		result = append(result, scanner.Text())
		i++
	}

	l.index = i

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return result, nil
}
