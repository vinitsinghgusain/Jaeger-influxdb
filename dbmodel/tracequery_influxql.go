package dbmodel

import (
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/jaeger-store/common"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

// InfluxQLTraceQuery abstracts an InfluxQL query for spans
type InfluxQLTraceQuery struct {
	measurement                string
	startTimeMin, startTimeMax time.Time
	durationMin, durationMax   time.Duration
	tags                       map[string]string
	numTraces                  int
}

// NewInfluxQLTraceQuery constructs a new InfluxQLTraceQuery
func NewInfluxQLTraceQuery(measurement string) *InfluxQLTraceQuery {
	return &InfluxQLTraceQuery{
		measurement: measurement,
		tags:        map[string]string{},
	}
}

// InfluxQLTraceQueryFromTQP constructs an InfluxQLTraceQuery using parameters in a spanstore.TraceQueryParameters
func InfluxQLTraceQueryFromTQP(measurement string, query *spanstore.TraceQueryParameters) *InfluxQLTraceQuery {
	q := NewInfluxQLTraceQuery(measurement)

	if query.ServiceName != "" {
		q.ServiceName(query.ServiceName)
	}
	if query.OperationName != "" {
		q.OperationName(query.OperationName)
	}
	if !query.StartTimeMin.IsZero() {
		q.StartTimeMin(query.StartTimeMin)
	}
	if !query.StartTimeMax.IsZero() {
		q.StartTimeMax(query.StartTimeMax)
	}
	for k, v := range query.Tags {
		q.Tag(k, v)
	}
	if query.DurationMin > 0 {
		q.DurationMin(query.DurationMin)
	}
	if query.DurationMax > 0 {
		q.DurationMax(query.DurationMax)
	}
	if query.NumTraces > 0 {
		q.NumTraces(query.NumTraces)
	}

	return q
}

// ServiceName sets the query service name.
func (q *InfluxQLTraceQuery) ServiceName(serviceName string) *InfluxQLTraceQuery {
	q.tags[common.ServiceNameKey] = serviceName
	return q
}

// OperationName sets the query operation name.
func (q *InfluxQLTraceQuery) OperationName(operationName string) *InfluxQLTraceQuery {
	q.tags[common.OperationNameKey] = operationName
	return q
}

// Tag adds a query tag key:value pair.
func (q *InfluxQLTraceQuery) Tag(k, v string) *InfluxQLTraceQuery {
	q.tags[k] = v
	return q
}

// StartTimeMin sets the min start time to query.
func (q *InfluxQLTraceQuery) StartTimeMin(startTimeMin time.Time) *InfluxQLTraceQuery {
	q.startTimeMin = startTimeMin
	return q
}

// StartTimeMax sets the max start time to query.
func (q *InfluxQLTraceQuery) StartTimeMax(startTimeMax time.Time) *InfluxQLTraceQuery {
	q.startTimeMax = startTimeMax
	return q
}

// DurationMax sets the query max duration threshold.
func (q *InfluxQLTraceQuery) DurationMax(durationMax time.Duration) *InfluxQLTraceQuery {
	q.durationMax = durationMax
	return q
}

// DurationMin sets the query min duration threshold.
func (q *InfluxQLTraceQuery) DurationMin(durationMin time.Duration) *InfluxQLTraceQuery {
	q.durationMin = durationMin
	return q
}

// NumTraces sets the query max traces threshold.
func (q *InfluxQLTraceQuery) NumTraces(numTraces int) *InfluxQLTraceQuery {
	q.numTraces = numTraces
	return q
}

// BuildTraceIDQuery builds an InfluxQL query that returns Trace IDs.
func (q *InfluxQLTraceQuery) BuildTraceIDQuery() string {
	var innerBuilder []string
	innerBuilder = append(innerBuilder, fmt.Sprintf(`select "%s" from %s`, common.DurationKey, q.measurement))

	var predicates []string
	for k, v := range q.tags {
		predicates = append(predicates, fmt.Sprintf(`"%s" = '%s'`, k, v))
	}
	if q.durationMin > 0 {
		predicates = append(predicates,
			fmt.Sprintf(`"%s" >= %d`, common.DurationKey, q.durationMin.Nanoseconds()))
	}
	if q.durationMax > 0 {
		predicates = append(predicates,
			fmt.Sprintf(`"%s" <= %d`, common.DurationKey, q.durationMax.Nanoseconds()))
	}
	if len(predicates) > 0 {
		innerBuilder = append(innerBuilder, fmt.Sprintf("where %s", strings.Join(predicates, " and ")))
	}

	innerBuilder = append(innerBuilder, fmt.Sprintf(`group by "%s" order by time desc limit 1`, common.TraceIDKey))

	innerQuery := strings.Join(innerBuilder, " ")

	var outerBuilder []string
	outerBuilder = append(outerBuilder, fmt.Sprintf(`select "%s" from (%s)`, common.TraceIDKey, innerQuery))

	predicates = nil

	if !q.startTimeMin.IsZero() {
		predicates = append(predicates,
			fmt.Sprintf(`time >= %d`, q.startTimeMin.UnixNano()))
	}
	if !q.startTimeMax.IsZero() {
		predicates = append(predicates,
			fmt.Sprintf(`time <= %d`, q.startTimeMax.UnixNano()))
	}
	if len(predicates) > 0 {
		outerBuilder = append(outerBuilder, fmt.Sprintf("where %s", strings.Join(predicates, " and ")))
	}

	outerBuilder = append(outerBuilder, fmt.Sprintf("order by time desc"))

	if q.numTraces > 0 {
		outerBuilder = append(outerBuilder, fmt.Sprintf(`limit %d`, q.numTraces))
	}

	return strings.Join(outerBuilder, " ")
}

// BuildTraceQuery builds a flux query that returns whole traces.
func (q *InfluxQLTraceQuery) BuildTraceQuery(traceIDs []model.TraceID) string {
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
