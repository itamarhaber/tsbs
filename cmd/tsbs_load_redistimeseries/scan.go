package main

import (
	"bufio"
	"log"
	"strings"
	"sync"
	radix "github.com/mediocregopher/radix"
	"github.com/timescale/tsbs/load"
)

type decoder struct {
	scanner *bufio.Scanner
}

// Reads and returns a text line that encodes a data point for a specif field name.
// Since scanning happens in a single thread, we hold off on transforming it
// to an INSERT statement until it's being processed concurrently by a worker.
func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		log.Fatalf("scan error: %v", d.scanner.Err())
	}
	return load.NewPoint(d.scanner.Text())
}

func sendRedisCommand(line string, conn *radix.Pool) {
	t := strings.Split(line, " ")
	//s := make([]interface{}, len(t))
	//for i, v := range t {
	//		s[i] = v
	//}
	err := conn.Do (radix.Cmd(nil, "TS.ADD", t...))
	if err != nil {
		log.Fatalf("TS.ADD failed: %s\n", err)
	}
}
type eventsBatch struct {
	rows []string
}

func (eb *eventsBatch) Len() int {
	return len(eb.rows)
}

func (eb *eventsBatch) Append(item *load.Point) {
	that := item.Data.(string)
	eb.rows = append(eb.rows, that)
}

var ePool = &sync.Pool{New: func() interface{} { return &eventsBatch{rows: []string{}} }}

type factory struct{}

func (f *factory) New() load.Batch {
	return ePool.Get().(*eventsBatch)
}
