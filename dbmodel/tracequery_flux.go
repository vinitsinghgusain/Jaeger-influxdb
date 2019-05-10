package dbmodel

import (
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/jaeger-influxdb/common"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

// FluxTraceQuery is a flux query builder that generates trace queries in Flux.
type FluxTraceQuery struct {
	bucket                          string
	spanMeasurement, logMeasurement string
	startTimeMin, startTimeMax      time.Time
	durationMin, durationMax        time.Duration
	tags                            map[string]string
	numTraces                       int
}

// NewFluxTraceQuery constructs a new FluxTraceQuery object.
func NewFluxTraceQuery(bucket, spanMeasurement, logMeasurement string, startTimeMin time.Time) *FluxTraceQuery {
	return &FluxTraceQuery{
		bucket:          bucket,
		spanMeasurement: spanMeasurement,
		logMeasurement:  logMeasurement,
		startTimeMin:    startTimeMin,
		tags:            make(map[string]string),
	}
}

// FluxTraceQueryFromTQP constructs a FluxTraceQuery using parameters in a spanstore.TraceQueryParameters
func FluxTraceQueryFromTQP(bucket, spanMeasurement, logMeasurement string, query *spanstore.TraceQueryParameters) *FluxTraceQuery {
	q := NewFluxTraceQuery(bucket, spanMeasurement, logMeasurement, query.StartTimeMin)

	if query.ServiceName != "" {
		q.ServiceName(query.ServiceName)
	}
	if query.OperationName != "" {
		q.OperationName(query.OperationName)
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
func (q *FluxTraceQuery) ServiceName(serviceName string) *FluxTraceQuery {
	q.tags[common.ServiceNameKey] = serviceName
	return q
}

// OperationName sets the query operation name.
func (q *FluxTraceQuery) OperationName(operationName string) *FluxTraceQuery {
	q.tags[common.OperationNameKey] = operationName
	return q
}

// Tag adds a query tag key:value pair.
func (q *FluxTraceQuery) Tag(k, v string) *FluxTraceQuery {
	q.tags[k] = v
	return q
}

// StartTimeMax sets the max start time to query.
func (q *FluxTraceQuery) StartTimeMax(startTimeMax time.Time) *FluxTraceQuery {
	q.startTimeMax = startTimeMax
	return q
}

// DurationMax sets the query max duration threshold.
func (q *FluxTraceQuery) DurationMax(durationMax time.Duration) *FluxTraceQuery {
	q.durationMax = durationMax
	return q
}

// DurationMin sets the query min duration threshold.
func (q *FluxTraceQuery) DurationMin(durationMin time.Duration) *FluxTraceQuery {
	q.durationMin = durationMin
	return q
}

// NumTraces sets the query max traces threshold.
func (q *FluxTraceQuery) NumTraces(numTraces int) *FluxTraceQuery {
	q.numTraces = numTraces
	return q
}

// BuildTraceQuery builds a flux query that returns whole traces.
func (q *FluxTraceQuery) BuildTraceQuery(traceIDs []model.TraceID) string {
	var builder []string
	builder = append(builder, fmt.Sprintf(`from(bucket: "%s")`, q.bucket))

	if !q.startTimeMax.IsZero() {
		builder = append(builder,
			fmt.Sprintf(
				`range(start: %s, stop: %s)`,
				q.startTimeMin.Add(-1 * time.Hour).UTC().Format(time.RFC3339Nano),
				q.startTimeMax.Add(time.Hour).UTC().Format(time.RFC3339Nano)))
	} else {
		builder = append(builder,
			fmt.Sprintf(
				`range(start: %s)`,
				q.startTimeMin.Add(-1 * time.Hour).UTC().Format(time.RFC3339Nano)))
	}

	measurementFilters := []string{
		fmt.Sprintf(`r.%s == "%s"`, common.MeasurementKey, q.spanMeasurement),
		fmt.Sprintf(`r.%s == "%s"`, common.MeasurementKey, q.logMeasurement),
	}

	traceIDFilters := make([]string, len(traceIDs))
	for i := range traceIDs {
		traceIDFilters[i] = fmt.Sprintf(`r.%s == "%s"`, common.TraceIDKey, traceIDs[i].String())
	}

	builder = append(builder,
		fmt.Sprintf("filter(fn: (r) => (%s) and (%s))",
			strings.Join(measurementFilters, " or "), strings.Join(traceIDFilters, " or ")))

	builder = append(builder,
		fmt.Sprintf(
			`pivot(rowKey:["%s"], columnKey:["%s"], valueColumn:"%s")`,
			common.TimeV2Key, common.FieldKey, common.ValueKey),
		fmt.Sprintf(`group(columns: ["%s", "%s"])`, common.MeasurementKey, common.TraceIDKey),
		`drop(columns: ["_start", "_stop"])`,
	)

	return strings.Join(builder, "\n |> ")
}

// BuildTraceIDQuery builds a flux query that returns Trace IDs.
func (q *FluxTraceQuery) BuildTraceIDQuery() string {
	var builder []string
	builder = append(builder, fmt.Sprintf(`from(bucket: "%s")`, q.bucket))

	if !q.startTimeMax.IsZero() {
		builder = append(builder,
			fmt.Sprintf(
				`range(start: %s, stop: %s)`,
				q.startTimeMin.UTC().Format(time.RFC3339Nano),
				q.startTimeMax.UTC().Format(time.RFC3339Nano)))
	} else {
		builder = append(builder,
			fmt.Sprintf(
				`range(start: %s)`,
				q.startTimeMin.UTC().Format(time.RFC3339Nano)))
	}

	filters := append(
		make([]string, 0, len(q.tags)+2),
		fmt.Sprintf(`r.%s == "%s"`, common.MeasurementKey, q.spanMeasurement),
		fmt.Sprintf(`r.%s == "%s"`, common.FieldKey, common.DurationKey))
	for k, v := range q.tags {
		filters = append(filters, fmt.Sprintf(`r.%s == "%s"`, k, v))
	}
	builder = append(builder,
		fmt.Sprintf("filter(fn: (r) => %s)", strings.Join(filters, " and ")),
		fmt.Sprintf(`group(columns:["%s"])`, common.TraceIDKey),
		"max()")

	if q.durationMin > 0 || q.durationMax > 0 {
		durationFilters := make([]string, 0, 2)
		if q.durationMin > 0 {
			durationFilters = append(durationFilters,
				fmt.Sprintf("r.%s >= %d", common.ValueKey, q.durationMin.Nanoseconds()))
		}
		if q.durationMax > 0 {
			durationFilters = append(durationFilters,
				fmt.Sprintf("r.%s <= %d", common.ValueKey, q.durationMax.Nanoseconds()))
		}

		builder = append(builder,
			fmt.Sprintf(`filter(fn: (r) => %s)`, strings.Join(durationFilters, " and ")),
			fmt.Sprintf(
				`pivot(rowKey:["%s"], columnKey:["%s"], valueColumn:"%s")`,
				common.TraceIDKey, common.FieldKey, common.ValueKey))
	}

	builder = append(builder,
		fmt.Sprintf(`keep(columns: ["%s", "%s"])`, common.TimeV2Key, common.TraceIDKey),
		`group()`,
		fmt.Sprintf(`sort(columns: ["%s"], desc: true)`, common.TimeV2Key),
	)

	if q.numTraces > 0 {
		builder = append(builder,
			fmt.Sprintf(`limit(n: %d)`, q.numTraces),
			fmt.Sprintf(`keep(columns: ["%s"])`, common.TraceIDKey))
	}

	return strings.Join(builder, "\n |> ")
}
