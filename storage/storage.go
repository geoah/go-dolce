package storage

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/superdecimal/dolce/config"
	"github.com/superdecimal/dolce/dolcelog"
)

type Database struct {
	DatabaseName string
	Filename     string
	Path         string
	File         *os.File
	Version      int
	Data         map[string][]byte
	dbMutex      sync.Mutex
	dlog         *dolcelog.DolceLog
}

func (d *Database) Set(key string, value string) error {
	d.dbMutex.Lock()
	defer d.dbMutex.Unlock()

	data := []byte(value)
	d.dlog.Set(key, data)
	d.Data[key] = data

	return nil
}

func (d *Database) Read(key string) (string, error) {
	return string(d.Data[key]), nil
}

func (d *Database) Delete(key string) (bool, error) {
	return false, nil
}

// New creates the db file and returns a pointer to it
func New(dl *dolcelog.DolceLog, databaseName string) (*Database, error) {
	db := &Database{
		dlog: dl,
	}

	_, err := os.Stat(config.DBFolder)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.Mkdir(config.DBFolder, 0777)
			if err != nil {
				fmt.Println(err)
				return nil, errors.New("error")
			}
		}
	}

	err = os.Chdir(config.DBFolder)
	if err != nil {
		fmt.Println(err)
	}

	f, err := os.Create(databaseName)
	if err != nil {
		fmt.Println(err)
	}

	defer f.Close()

	db.DatabaseName = databaseName
	db.Filename = databaseName
	db.Version = 001
	db.File = f
	db.Data = make(map[string][]byte, 1000)

	wr := bufio.NewWriter(f)

	_, err = fmt.Fprintf(wr, "DolceDB.%d", config.DBVersion)
	if err != nil {
		fmt.Println(err)
	}
	wr.Flush()

	return db, nil
}

func DeleteDBFile(db string) bool {
	err := os.Remove(db)
	if err != nil {
		return false
	}
	return true

}

func ListDBs() {

}

// RebuildMap is rebuilds the in memory map from the log
func (d *Database) RebuildMap() {
	d.dbMutex.Lock()
	defer d.dbMutex.Unlock()

	temp, err := d.dlog.GetAll()
	if err != nil {
		log.Fatal("Could not get log.")
		return
	}

	for _, entry := range temp {
		var key, value, action string
		var ind int

		in := strings.NewReader(entry)
		fmt.Fscanf(in, "%d %s %s %s", &ind, &action, &key, &value)

		d.Data[key] = []byte(value)
	}
}
