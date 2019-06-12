package serialize

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
)

// RedisTimeSeriesSerializer writes a Point in a serialized form for RedisTimeSeries
type RedisTimeSeriesSerializer struct{}

var keysSoFar map[string]bool

// Serialize writes Point data to the given writer, in a format that will be easy to create a redis-timeseries command
// from.
//
// This function writes output that looks like:
//cpu_usage_user{md5(hostname=host_0|region=eu-central-1...)} 1451606400 58 LABELS hostname host_0 region eu-central-1 ... measurement cpu fieldname usage_user
//
// Which the loader will decode into a set of TS.ADD commands for each fieldKey. Once labels have been created for a each fieldKey,
// subsequent rows are ommitted with them and are ingested with TS.MADD for a row's metrics.
func (s *RedisTimeSeriesSerializer) Serialize(p *Point, w io.Writer) (err error) {
	if keysSoFar == nil {
		keysSoFar = make(map[string]bool)
	}
	// Construct labels text, prefixed with name 'LABELS', following pairs of label names and label values
	// This will be added to each new key, with additional "fieldname" tag
	labels := make([]byte, 0, 256)
	labelsForKeyName := make([]byte, 0, 256)
	labels = append(labels, []byte(" LABELS")...)
	for i, v := range p.tagValues {
		labels = append(labels, ' ')
		labels = append(labels, p.tagKeys[i]...)
		labels = append(labels, ' ')
		labels = append(labels, v...)

		// construct a string of {hostname=host_1,region=us-west-1,...} to be used for unique name for key
		if i > 0 {
			labelsForKeyName = append(labelsForKeyName, '|')
		} else {
			labelsForKeyName = append(labelsForKeyName, '{')
		}
		labelsForKeyName = append(labelsForKeyName, p.tagKeys[i]...)
		labelsForKeyName = append(labelsForKeyName, '=')
		labelsForKeyName = append(labelsForKeyName, v...)
	}

	if len(labelsForKeyName) > 0 {
		labelsForKeyName = append(labelsForKeyName, '}')
	}
	// add measurement name as additional label to be used in queries
	labels = append(labels, []byte(" measurement ")...)
	labels = append(labels, p.measurementName...)

	// Write new line for each fieldKey in the form of: measurementName_fieldName{md5 of labels} timestamp fieldValue LABELS ....
	buf := make([]byte, 0, 256)
	for fieldID := 0; fieldID < len(p.fieldKeys); fieldID++ {
		fieldName := p.fieldKeys[fieldID]
		fieldValue := p.fieldValues[fieldID]
		keyName := fmt.Sprintf("%s_%s%s", p.measurementName, fieldName, labelsForKeyName)
		// write unique key name
		labelsHash := md5.Sum([]byte(labelsForKeyName))
		buf = append(buf, p.measurementName...)
		buf = append(buf, '_')
		buf = append(buf, fieldName...)

		buf = append(buf, '{')
		buf = fastFormatAppend(int(binary.BigEndian.Uint32(labelsHash[:])), buf)
		buf = append(buf, '}')

		buf = append(buf, ' ')

		// write timestamp
		buf = fastFormatAppend(p.timestamp.UTC().Unix(), buf)
		buf = append(buf, ' ')
		// write value
		buf = fastFormatAppend(fieldValue, buf)

		// if this key was already inserted and created, we don't to specify the labels again
		if keysSoFar[keyName] {
			buf = append(buf, ' ')
			continue
		}
		keysSoFar[keyName] = true
		buf = append(buf, labels...)
		// additional label of fieldname
		buf = append(buf, []byte(" fieldname ")...)
		buf = fastFormatAppend(fieldName, buf)
		buf = append(buf, '\n')
	}
	if buf[len(buf)-1] == ' ' {
		buf[len(buf)-1] = '\n'
	}
	_, err = w.Write(buf)

	return err
}
