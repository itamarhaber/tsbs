package main

import (
	radix "github.com/mediocregopher/radix"
	"log"
	"runtime"
	)

type dbCreator struct {
	client *radix.Pool

}

func (d *dbCreator) Init() {

	// multiply parallel with GOMAXPROCS to get the actual number of goroutines and thus
	// connections needed for the benchmarks.
	parallel := runtime.GOMAXPROCS(0)
	poolSize := parallel * runtime.GOMAXPROCS(0)
	d.client, _ = radix.NewPool("tcp", host, poolSize, nil)
	log.Print("Using pool size of ", poolSize )

}

func (d *dbCreator) DBExists(dbName string) bool {
	return true
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	err := d.client.Do(radix.Cmd(nil, "FLUSHALL"))
	 return err
}

func (d *dbCreator) CreateDB(dbName string) error {
	return nil
}

func (d *dbCreator) Close() {
	d.client.Close()
}
