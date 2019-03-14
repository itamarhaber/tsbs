package main

import (
	"bufio"
	"flag"
	"github.com/timescale/tsbs/load"
	"log"
)

// Program option vars:
var (
	host        string
)

// Global vars
var (
	loader *load.BenchmarkRunner
	//bufPool sync.Pool
)
// allows for testing
var fatal = log.Fatal

// Parse args:
func init() {
	loader = load.GetBenchmarkRunnerWithBatchSize(1000)
	flag.StringVar(&host, "host", "localhost:6379", "Provide host:port for redis connection")
	flag.Parse()
}

type benchmark struct {
	dbc *dbCreator
}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	return &load.ConstantIndexer{}
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
	var err error
	events := b.(*eventsBatch)
	if doLoad {
		conn := p.dbc.client.Pool.Get()
		for _, row := range events.rows {
			sendRedisCommand(row, conn)
		}
		err = conn.Flush()
		if err != nil {
			log.Fatalf("Error while inserting: %v", err)
		}

	}
	rowCnt := uint64(len(events.rows))
	metricCnt := rowCnt
	events.rows = events.rows[:0]
	ePool.Put(events)
	return metricCnt, rowCnt
}


func main() {
	loader.RunBenchmark(&benchmark{dbc: &dbCreator{}}, load.SingleQueue)
}
