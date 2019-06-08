package serialize

import (
	"crypto/md5"
	"encoding/binary"
	"io"
)

// RedisZsetMetricSerializer writes a Point in a serialized form for zsets
type RedisZsetMetricSerializer struct{}

// Serialize writes Point data to the given writer, in a format that will be easy to create a redis-timeseries command
// from.
func (s *RedisZsetMetricSerializer) Serialize(p *Point, w io.Writer) (err error) {
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

	// Write new line for each device in the form of: deviceName{md5 of labels} timestamp timestamp:value:...
	buf := make([]byte, 0, 256)
	for fieldID := 0; fieldID < len(p.fieldKeys); fieldID++ {
		fieldName := p.fieldKeys[fieldID]
		fieldValue := p.fieldValues[fieldID]
		// write unique key name
		labelsHash := md5.Sum([]byte(labelsForKeyName))
		buf = append(buf, []byte("ZADD ")...)
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
		buf = fastFormatAppend(p.timestamp.UTC().Unix(), buf)
		buf = append(buf, ':')
		buf = fastFormatAppend(fieldValue, buf)
		buf = append(buf, '\n')
	}
	_, err = w.Write(buf)

	return err
}
