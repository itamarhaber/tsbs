package serialize
import (
	"io"
)
// RedisTimeSeriesSerializer writes a Point in a serialized form for RedisTimeSeries
type RedisTimeSeriesSerializer struct{}

// Serialize writes Point data to the given writer, in a format that will be easy to create a redis-timeseries command
// from.
//
// This function writes output that looks like:
//cpu_usage_user{hostname=host_0|region=eu-central-1...} 1451606400000000000 58 LABELS hostname host_0 region eu-central-1 ... measurement cpu fieldname usage_user
//
// Which the loader will decode into a set of TS.ADD commands for each fieldKey.
func (s *RedisTimeSeriesSerializer) Serialize(p *Point, w io.Writer) (err error) {
	// Construct labels text, prefixed with name 'LABELS', following pairs of label names and label values
	// This will be added to each key, with additional "fieldname" tag
	labels := make([]byte, 0, 256)
	labelsForKeyName:= make([]byte, 0, 256)
	labels = append(labels, []byte(" LABELS")...)
	for i, v := range p.tagValues {
		labels = append(labels, ' ')
		labels = append(labels, p.tagKeys[i]...)
		labels = append(labels, ' ')
		labels = append(labels, v...)

		// construct a string of {hostname=host_1,region=us-west-1,...} to be used as unique name for key
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
	labels = append(labels, []byte(" measurement ")... )
	labels = append(labels, p.measurementName...)

	// Write new line for each fieldKey in the form of: measurementName_fieldName timestamp fieldValue LABELS ....
	buf := make([]byte, 0, 256)
	for fieldID := 0; fieldID < len(p.fieldKeys); fieldID++ {
		fieldName := p.fieldKeys[fieldID]
		fieldValue := p.fieldValues[fieldID]
		// write unique key name
		buf = append(buf, p.measurementName...)
		buf = append(buf, '_')
		buf = fastFormatAppend(fieldName, buf)
		buf = append(buf, labelsForKeyName...)
		buf = append(buf, ' ')
		// write timestamp
		buf = fastFormatAppend(p.timestamp.UTC().Unix(), buf)
		buf = append(buf, ' ')
		// write value
		buf = fastFormatAppend(fieldValue, buf)
		buf = append(buf, labels...)
		// additional label of fieldname
		buf = append(buf, []byte(" fieldname ")...)
		buf = fastFormatAppend(fieldName, buf)
		buf = append(buf, '\n')
	}

	_, err = w.Write(buf)
	return err
}

