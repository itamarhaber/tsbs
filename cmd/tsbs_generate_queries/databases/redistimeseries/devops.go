package redistimeseries

import (
	"fmt"
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
// every 5 mins for 1 hour
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.RandWindow(timeRange)
	metrics := devops.GetCPUMetricsSlice(numMetrics)
	metric := metrics[0]
	hostnames := d.GetRandomHosts(nHosts)

	redisQuery := fmt.Sprintf(`TS.MRANGE %d %d AGGREGATION max %d FILTER hostname=%s fieldname=%s`,
		interval.Start.Unix(),
		interval.End.Unix(),
		oneMinute * 5,
		hostnames[0],
		metric)
	humanLabel := fmt.Sprintf("RedisTimeSeries %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	//todo: implement for redistimeseries
	/*
	metrics := devops.GetCPUMetricsSlice(numMetrics)
	interval := d.Interval.RandWindow(devops.DoubleGroupByDuration)

	selectClauses := make([]string, numMetrics)
	meanClauses := make([]string, numMetrics)
	for i, m := range metrics {
		meanClauses[i] = "mean_" + m
		selectClauses[i] = fmt.Sprintf("avg(%s) as %s", m, meanClauses[i])
	}

	hostnameField := "hostname"
	joinStr := ""
	if d.UseJSON || d.UseTags {
		if d.UseJSON {
			hostnameField = "tags->>'hostname'"
		} else if d.UseTags {
			hostnameField = "tags.hostname"
		}
		joinStr = "JOIN tags ON cpu_avg.tags_id = tags.id"
	}

	sql := fmt.Sprintf(`
        WITH cpu_avg AS (
          SELECT %s as hour, tags_id,
          %s
          FROM cpu
          WHERE time >= '%s' AND time < '%s'
          GROUP BY hour, tags_id
        )
        SELECT hour, %s, %s
        FROM cpu_avg
        %s
        ORDER BY hour, %s`,
		d.getTimeBucket(oneHour),
		strings.Join(selectClauses, ", "),
		interval.Start.Format(goTimeFmt), interval.End.Format(goTimeFmt),
		hostnameField, strings.Join(meanClauses, ", "),
		joinStr, hostnameField)
	humanLabel := devops.GetDoubleGroupByLabel("RedisTimeSeries", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
	*/
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
