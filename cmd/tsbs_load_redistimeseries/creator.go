package main

import (
	"github.com/mediocregopher/radix"
	"log"
)

type dbCreator struct {
	client *radix.Pool
}

func (d *dbCreator) Init() {

	// multiply parallel with GOMAXPROCS to get the actual number of goroutines and thus
	// connections needed for the benchmarks.

	poolOptions := []radix.PoolOpt{
		radix.PoolPipelineConcurrency(poolPipelineConcurrency),
	}
	d.client, _ = radix.NewPool("tcp", host, poolSize, poolOptions...)
	log.Print("Using pool size of ", poolSize)
	log.Print("Using PoolPipelineConcurrency size of ", poolPipelineConcurrency)

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
