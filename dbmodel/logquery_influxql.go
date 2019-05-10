package dbmodel

import (
	"fmt"
	"strings"

	"github.com/influxdata/jaeger-influxdb/common"
	"github.com/jaegertracing/jaeger/model"
)

// InfluxQLLogQuery abstracts an InfluxQL query for span logs
type InfluxQLLogQuery struct {
	measurement string
}

// NewInfluxQLLogQuery constructs a new InfluxQLLogQuery
func NewInfluxQLLogQuery(measurement string) *InfluxQLLogQuery {
	return &InfluxQLLogQuery{
		measurement: measurement,
	}
}

// BuildLogQuery builds a InfluxQL query
func (q *InfluxQLLogQuery) BuildLogQuery(traceIDs []model.TraceID) string {
	var builder []string
	builder = append(builder, fmt.Sprintf(`select * from %s where`, q.measurement))

	predicates := make([]string, len(traceIDs))
	for i, traceID := range traceIDs {
		predicates[i] = fmt.Sprintf(`"%s" = '%s'`, common.TraceIDKey, traceID.String())
	}
	builder = append(builder,
		fmt.Sprintf(`%s`, strings.Join(predicates, " or ")),
		"group by trace_id")

	return strings.Join(builder, " ")
}
