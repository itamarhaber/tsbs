package main

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"strings"

	"github.com/timescale/tsbs/load"
)

// Program option vars:
var (
	host string
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
	return &processor{b.dbc}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return b.dbc
}

type processor struct {
	dbc *dbCreator
}

func (p *processor) Init(_ int, _ bool) {}

// ProcessBatch reads eventsBatches which contain rows of data for TS.ADD redis command string
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	events := b.(*eventsBatch)
	cmdLen := 0
	if doLoad {
		conn := p.dbc.client.Pool.Get()
		for _, row := range events.rows {
			cmds := strings.Split(row, ";")
			for i := range cmds {
				if strings.TrimSpace(cmds[i]) == "" {
					continue
				}
				sendRedisCommand(cmds[i], conn)
				cmdLen++
			}
		}
		err := conn.Flush()
		if err != nil {
			log.Fatalf("Error while inserting: %v", err)
		}

	}
	rowCnt := uint64(len(events.rows))
	events.rows = events.rows[:0]
	ePool.Put(events)
	return uint64(cmdLen), rowCnt
}

func main() {
	loader.RunBenchmark(&benchmark{dbc: &dbCreator{}}, load.WorkerPerQueue)
}
