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
//LABELS hostname host_0 region eu-central-1 datacenter eu-central-1a rack 6 os Ubuntu15.10 arch x86 team SF service 19
//cpu_usage_user{hostname=host_0,region=eu-central-1...} 1451606400000000000 58,cpu_usage_system 1451606400000000000 2,cpu_usage_idle 1451606400000000000 24
//
// Which the loader will decode into a set of TS.ADD commands for each fieldKey.
func (s *RedisTimeSeriesSerializer) Serialize(p *Point, w io.Writer) (err error) {
	// Labels row first, prefixed with name 'LABELS', following pairs of label names and label values
	buf := make([]byte, 0, 256)
	labelsForKeyName:= make([]byte, 0, 256)
	buf = append(buf, []byte("LABELS")...)
	for i, v := range p.tagValues {
		buf = append(buf, ' ')
		buf = append(buf, p.tagKeys[i]...)
		buf = append(buf, ' ')
		buf = append(buf, v...)

		// construct a string of {hostname=host_1,region=us-west-1,...} to be used as unique name for key
		if i > 0 {
			labelsForKeyName = append(labelsForKeyName, ',')
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
	buf = append(buf, []byte(" measurement ")... )
	buf = append(buf, p.measurementName...)
	buf = append(buf, '\n')
	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	// Write comma separated string for each fieldKey in the form of: measurementName_fieldName timestamp fieldValue
	buf = make([]byte, 0, 256)
	for fieldID := 0; fieldID < len(p.fieldKeys); fieldID++ {
		fieldName := p.fieldKeys[fieldID]
		fieldValue := p.fieldValues[fieldID]
		if fieldID > 0 {
			buf = append(buf, ',')
		}

		buf = append(buf, p.measurementName...)
		buf = append(buf, '_')
		buf = fastFormatAppend(fieldName, buf)
		buf = append(buf, labelsForKeyName...)
		buf = append(buf, ' ')
		buf = fastFormatAppend(p.timestamp.UTC().UnixNano(), buf)
		buf = append(buf, ' ')
		buf = fastFormatAppend(fieldValue, buf)
	}

	buf = append(buf, '\n')
	_, err = w.Write(buf)
	return nil
}

