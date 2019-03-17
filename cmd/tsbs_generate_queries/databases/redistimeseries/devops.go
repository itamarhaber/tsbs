package redistimeseries

import (
	"fmt"
	"log"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

const (
	oneMinute = 60
	oneHour   = oneMinute * 60
)

// Devops produces RedisTimeSeries-specific queries for all the devops query types.
type Devops struct {
	*devops.Core
}

// NewDevops makes an Devops object ready to generate Queries.
func NewDevops(start, end time.Time, scale int) *Devops {
	return &Devops{devops.NewCore(start, end, scale)}
}

// GenerateEmptyQuery returns an empty query.RedisTimeSeries
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewRedisTimeSeries()
}


// GroupByTime fetches the MAX for numMetrics metrics under 'cpu', per minute for nhosts hosts,
// every 1 mins for 1 hour
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	if numMetrics > 1 || nHosts > 1 {
		log.Fatal("Not supported for more than 1 hostname/ 1 metric")
	}
	interval := d.Interval.RandWindow(timeRange)
	metrics := devops.GetCPUMetricsSlice(numMetrics)
	metric := metrics[0]
	hostnames := d.GetRandomHosts(nHosts)

	redisQuery := fmt.Sprintf(`TS.MRANGE %d %d AGGREGATION max %d FILTER hostname=%s fieldname=%s`,
		interval.Start.Unix(),
		interval.End.Unix(),
		oneMinute,
		hostnames[0],
		metric)
	humanLabel := fmt.Sprintf("RedisTimeSeries %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// GroupByTimeAndPrimaryTag selects the AVG of one cpu metric/all cpu metrics per device per hour for 12 hours
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	if numMetrics != 1 && numMetrics != devops.GetCPUMetricsLen() {
		log.Fatal("Supports only 1 cpu metric or all cpu metrics")
	}
	interval := d.Interval.RandWindow(devops.DoubleGroupByDuration)

	redisQuery := fmt.Sprintf(`TS.MRANGE %d %d AGGREGATION avg %d FILTER measurement=cpu`,
		interval.Start.Unix(),
		interval.End.Unix(),
		oneHour)

	// add specific fieldname if needed. Currently only one cpu metric is supported.
	if numMetrics == 1 {
		redisQuery += " fieldname="
		redisQuery += devops.GetCPUMetricsSlice(1)[0]
	}

	humanLabel := devops.GetDoubleGroupByLabel("RedisTimeSeries", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// MaxAllCPU fetches the aggregate across all CPU metrics per hour over 1 hour for a single host.
// Currently only one host is supported
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.RandWindow(devops.MaxAllDuration)
	hostnames := d.GetRandomHosts(nHosts)

	redisQuery := fmt.Sprintf(`TS.MRANGE %d %d AGGREGATION max %d FILTER measurement=cpu hostname=%s`,
		interval.Start.Unix(),
		interval.End.Unix(),
		oneHour,
		//currently support only one host
		hostnames[0])

	humanLabel := devops.GetMaxAllLabel("RedisTimeSeries", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// fill Query fills the query struct with data
func (d *Devops) fillInQuery(qi query.Query, humanLabel, humanDesc, redisQuery string) {
	q := qi.(*query.RedisTimeSeries)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.RedisQuery = []byte(redisQuery)
}
