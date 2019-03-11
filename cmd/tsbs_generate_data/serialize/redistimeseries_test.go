package serialize
import (
	"testing"
)

func TestRedisTimeSeriesSerializer(t *testing.T) {
	cases := []serializeCase{
		{
			desc:       "a regular Point",
			inputPoint: testPointDefault,
			output:     "LABELS hostname host_0 region eu-west-1 datacenter eu-west-1b measurement cpu\ncpu_usage_guest_nice{hostname=host_0,region=eu-west-1,datacenter=eu-west-1b} 1451606400000000000 38.24311829\n",
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: testPointInt,
			output:     "LABELS hostname host_0 region eu-west-1 datacenter eu-west-1b measurement cpu\ncpu_usage_guest{hostname=host_0,region=eu-west-1,datacenter=eu-west-1b} 1451606400000000000 38\n",
		},
		{
			desc:       "a regular Point with multiple fields",
			inputPoint: testPointMultiField,
			output:     "LABELS hostname host_0 region eu-west-1 datacenter eu-west-1b measurement cpu\ncpu_big_usage_guest{hostname=host_0,region=eu-west-1,datacenter=eu-west-1b} 1451606400000000000 5000000000,cpu_usage_guest{hostname=host_0,region=eu-west-1,datacenter=eu-west-1b} 1451606400000000000 38,cpu_usage_guest_nice{hostname=host_0,region=eu-west-1,datacenter=eu-west-1b} 1451606400000000000 38.24311829\n",
		},
		{
			desc:       "a Point with no tags",
			inputPoint: testPointNoTags,
			output:     "LABELS measurement cpu\ncpu_usage_guest_nice 1451606400000000000 38.24311829\n",
		},
	}

	testSerializer(t, cases, &RedisTimeSeriesSerializer{})
}
