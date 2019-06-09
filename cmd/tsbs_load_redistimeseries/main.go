package main

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"flag"
	"github.com/mediocregopher/radix"
	"github.com/timescale/tsbs/load"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
)

// Program option vars:
var (
	host string
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
	flag.Uint64Var(&connections, "connections", 1, "Provide the number of connections per worker")
	flag.Uint64Var(&pipeline, "pipeline", 50, "Provide the pipeline size")
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

func (p *processor) Init(_ int, _ bool) {}

func rtsAdder(wg *sync.WaitGroup, rows chan string, metrics chan uint64, conn *radix.Pool, load uint64, id uint64) {
	curPipe := uint64(0)
	// log.Printf("rts %v starts", id)
	pipeline_commands := []radix.CmdAction{}
	for i := uint64(0); i < load; i++ {
		// log.Printf("rts %v reads row", id)
		row := <-rows
		cmds := strings.Split(row, ";")

		for i := range cmds {
			if strings.TrimSpace(cmds[i]) == "" {
				continue
			}
			t := strings.Split(cmds[i], " ")
			pipeline_commands = append(pipeline_commands, radix.Cmd(nil, "TS.ADD", t...))

			curPipe++
			if curPipe >= pipeline {
				pipeline := radix.Pipeline(
					pipeline_commands...
				)
				if err := conn.Do(pipeline); err != nil {
					log.Fatalf("Error while inserting: ", err)
				}
				pipeline_commands = []radix.CmdAction{}
				metrics <- curPipe
				curPipe = 0
			}

		}
	}
	if curPipe > 0 {
		pipeline := radix.Pipeline(
			pipeline_commands...
		)
		if err := conn.Do(pipeline); err != nil {
			log.Fatalf("Error while inserting: ", err)
		}

		metrics <- curPipe
	}
	wg.Done()
}

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
			//conn := p.dbc.client.Pool.Get()
			//defer conn.Close()
			p.wg.Add(1)
			if i == connections-1 {
				load += rowCnt & connections
			}
			go rtsAdder(p.wg, p.rows, p.metrics, p.dbc.client, load, i)
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

	}
	events.rows = events.rows[:0]
	ePool.Put(events)
	return metricCnt, rowCnt

}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	loader.RunBenchmark(&benchmark{dbc: &dbCreator{}}, load.WorkerPerQueue)
}
