package main

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/gomodule/redigo/redis"
	"github.com/timescale/tsbs/load"
)

// Program option vars:
var (
	host        string
	connections uint64
	pipeline    uint64
)

// Global vars
var (
	loader *load.BenchmarkRunner
	//bufPool sync.Pool
)

// allows for testing
var fatal = log.Fatal
var md5h = md5.New()

// Parse args:
func init() {
	loader = load.GetBenchmarkRunnerWithBatchSize(1000)
	flag.StringVar(&host, "host", "localhost:6379", "Provide host:port for redis connection")
	flag.Uint64Var(&connections, "connections", 10, "Provide the number of connections per worker")
	flag.Uint64Var(&pipeline, "pipline", 50, "Provide the pipeline size")
	flag.Parse()
}

type benchmark struct {
	dbc *dbCreator
}

type RedisIndexer struct {
	partitions uint
}

func (i *RedisIndexer) GetIndex(p *load.Point) int {
	row := p.Data.(string)
	key := strings.Split(row, " ")[0]
	start := strings.Index(key, "{")
	end := strings.Index(key, "}")
	_, _ = io.WriteString(md5h, key[start+1:end])
	hash := binary.LittleEndian.Uint32(md5h.Sum(nil))
	md5h.Reset()
	return int(uint(hash) % i.partitions)
}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	return &RedisIndexer{partitions: maxPartitions}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{b.dbc, nil, nil, nil}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return b.dbc
}

type processor struct {
	dbc     *dbCreator
	rows    chan string
	metrics chan uint64
	wg      *sync.WaitGroup
}

func rtsAdder(wg *sync.WaitGroup, rows chan string, metrics chan uint64, conn redis.Conn, load uint64, id uint64) {
	curPipe := uint64(0)
	// log.Printf("rts %v starts", id)
	for i := uint64(0); i < load; i++ {
		// log.Printf("rts %v reads row", id)
		row := <-rows
		cmds := strings.Split(row, ";")
		for j := range cmds {
			if strings.TrimSpace(cmds[j]) == "" {
				continue
			}
			curPipe++
			sendRedisCommand(cmds[j], conn)
		}

		if curPipe >= pipeline {
			conn.Flush()
			for k := uint64(0); k < curPipe; k++ {
				conn.Receive()
			}
			metrics <- curPipe
			curPipe = 0
			// log.Printf("rts %v flush", id)
		}
	}
	if curPipe > 0 {
		conn.Flush()
		for k := uint64(0); k < curPipe; k++ {
			conn.Receive()
		}
		metrics <- curPipe
	}
	wg.Done()
}

func (p *processor) Init(_ int, _ bool) {}

// ProcessBatch reads eventsBatches which contain rows of data for TS.ADD redis command string
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	events := b.(*eventsBatch)
	rowCnt := uint64(len(events.rows))
	metricCnt := uint64(0)
	if doLoad {
		buflen := rowCnt + 1
		p.rows = make(chan string, buflen)
		p.metrics = make(chan uint64, buflen*10)
		p.wg = &sync.WaitGroup{}
		load := rowCnt / connections
		for i := uint64(0); i < connections; i++ {
			conn := p.dbc.client.Pool.Get()
			defer conn.Close()
			p.wg.Add(1)
			if i == connections-1 {
				load += rowCnt & connections
			}
			go rtsAdder(p.wg, p.rows, p.metrics, conn, load, i)
		}

		for _, row := range events.rows {
			p.rows <- row
		}

		close(p.rows)
		p.wg.Wait()
		close(p.metrics)

		for val := range p.metrics {
			metricCnt += val
		}
		// err := conn.Flush()
		// if err != nil {
		// 	log.Fatalf("Error while inserting: %v", err)
		// }

		// for i := 0; i < cmdLen; i++ {
		// 	_, err = conn.Receive()
		// 	if err != nil {
		// 		log.Fatalf("Error while inserting: %v, cmd: '%s'", err, sent[i])
		// 	}
		// }
	}
	events.rows = events.rows[:0]
	ePool.Put(events)
	return metricCnt, rowCnt
}

func (p *processor) Close(_ bool) {
}

func verifyRun() {
	conn, err := redis.DialURL(fmt.Sprintf("redis://%s", host))
	if err != nil {
		log.Fatalf("Error while dialing %v", err)
	}
	_, err = conn.Do("PING")
	if err != nil {
		log.Fatalf("Error while PING %v", err)
	}

	cursor := 0
	total := 0
	for {
		rep, _ := redis.Values(conn.Do("SCAN", cursor))
		cursor, _ = redis.Int(rep[0], nil)
		keys, _ := redis.Strings(rep[1], nil)
		for _, key := range keys {
			total++
			info, _ := redis.Values(conn.Do("TS.INFO", key))
			chunks, _ := redis.Int(info[5], nil)
			if chunks != 30 {
				log.Printf("Verification error: key %v has %v chunks\n", key, chunks)
			}
		}
		if cursor == 0 {
			break
		}
	}
	log.Printf("Verified %v keys\n", total)
}

func main() {
	loader.RunBenchmark(&benchmark{dbc: &dbCreator{}}, load.WorkerPerQueue)
	verifyRun()
}
