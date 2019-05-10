package dbmodel

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/influxdata/influxdb1-client"
	"github.com/influxdata/influxdb1-client/models"
	"github.com/influxdata/jaeger-influxdb/common"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/multierror"
	"github.com/pkg/errors"
)

// DependencyLinksFromResultV1 converts an InfluxDB response to a dependency graph
func DependencyLinksFromResultV1(response *client.Response) ([]model.DependencyLink, error) {
	parentByChild := make(map[model.SpanID]model.SpanID)
	serviceNameBySpanID := make(map[model.SpanID]string)

	for _, result := range response.Results {
		if result.Err != nil {
			return nil, result.Err
		}
		for _, row := range result.Series {
			spanColI, serviceNameColI, referencesColI := -1, -1, -1

			for i, c := range row.Columns {
				switch c {
				case common.SpanIDKey:
					spanColI = i
				case common.ServiceNameKey:
					serviceNameColI = i
				case common.ReferencesKey:
					referencesColI = i
				}
			}

			for _, v := range row.Values {
				spanID, err := model.SpanIDFromString(v[spanColI].(string))
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to parse SpanID '%s'", v[spanColI].(string))
				}
				serviceName := v[serviceNameColI].(string)
				serviceNameBySpanID[spanID] = serviceName

				var references string
				if referencesColI >= 0 && v[referencesColI] != nil {
					references = v[referencesColI].(string)
				}
				refs, err := referencesFromString(references)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to parse references '%s'", references)
				}
				for _, ref := range refs {
					if ref.RefType == model.SpanRefType_CHILD_OF {
						parentByChild[spanID] = ref.SpanID
					}
				}
			}
		}
	}

	m := make(map[string]*model.DependencyLink)
	for child, parent := range parentByChild {
		dl := m[child.String()+parent.String()]
		if dl == nil {
			dl = &model.DependencyLink{
				Parent:    serviceNameBySpanID[parent],
				Child:     serviceNameBySpanID[child],
				CallCount: 0,
			}
			m[child.String()+parent.String()] = dl
		}
		dl.CallCount++
	}

	dependencyLinks := make([]model.DependencyLink, 0, len(m))
	for _, dl := range m {
		if dl.Parent == dl.Child {
			continue
		}
		dependencyLinks = append(dependencyLinks, *dl)
	}

	return dependencyLinks, nil
}

// TraceIDsFromInfluxQLResult converts an InfluxDB result to a slice of TraceIDs
func TraceIDsFromInfluxQLResult(response *client.Response) ([]model.TraceID, error) {
	var traceIDs []model.TraceID

	for _, result := range response.Results {
		if result.Err != nil {
			return nil, result.Err
		}
		for _, row := range result.Series {
			traceColI := -1

			for i, c := range row.Columns {
				switch c {
				case common.TraceIDKey:
					traceColI = i
				}
			}

			if traceIDs == nil {
				traceIDs = make([]model.TraceID, 0, len(row.Values))
			}
			for _, v := range row.Values {
				traceID, err := model.TraceIDFromString(v[traceColI].(string))
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to get traceID from flux value '%s'", v[traceColI].(string))
				}
				traceIDs = append(traceIDs, traceID)
			}
		}
	}

	return traceIDs, nil
}

// TracesFromInfluxQLResponse converts an InfluxQL response to Jaeger traces.
func TracesFromInfluxQLResponse(response *client.Response) ([]*model.Trace, error) {
	var traces []*model.Trace
	for _, result := range response.Results {
		for _, row := range result.Series {
			trace, err := TraceFromInfluxQLRow(&row)
			if err != nil {
				return nil, err
			}
			traces = append(traces, trace)
		}
	}
	return traces, nil
}

// TraceFromInfluxQLRow converts an InfluxQL result row to Jaeger traces.
func TraceFromInfluxQLRow(row *models.Row) (*model.Trace, error) {
	var errs []error

	var traceID model.TraceID
	var err error
	for k, v := range row.Tags {
		if k == common.TraceIDKey {
			traceID, err = model.TraceIDFromString(v)
			if err != nil {
				errs = append(errs, err)
			}
			break
		}
	}
	if traceID.High == 0 && traceID.Low == 0 {
		errs = append(errs, errors.New("trace_id tag not found"))
	}

	timeColI, serviceNameColI, operationNameColI, spanIDColI, durationColI, flagsColI, processTagKeysColI, referencesColI :=
		-1, -1, -1, -1, -1, -1, -1, -1
	jaegerTagKeysByColI := make(map[int]string)

	for colI, col := range row.Columns {
		switch col {
		case common.TimeV1Key:
			timeColI = colI
		case common.ServiceNameKey:
			serviceNameColI = colI
		case common.OperationNameKey:
			operationNameColI = colI
		case common.SpanIDKey:
			spanIDColI = colI
		case common.DurationKey:
			durationColI = colI
		case common.FlagsKey:
			flagsColI = colI
		case common.ProcessTagKeysKey:
			processTagKeysColI = colI
		case common.ReferencesKey:
			referencesColI = colI

		default:
			parts := strings.SplitN(col, ":", 2)
			if len(parts) < 2 {
				errs = append(errs, fmt.Errorf("unrecognized key '%s'", col))
				continue
			}
			prefix, key := parts[0], parts[1]

			switch prefix {
			case common.TagKeyPrefix:
				jaegerTagKeysByColI[colI] = key
			default:
				errs = append(errs, fmt.Errorf("unrecognized field key prefix '%s' in key '%s", key, col))
			}
		}
	}
	if len(errs) > 0 {
		return nil, multierror.Wrap(errs)
	}

	var trace model.Trace

	for _, v := range row.Values {
		if timeColI == -1 {
			errs = append(errs, errors.New("time column not found"))
			continue
		}
		startTime, err := time.Parse(time.RFC3339Nano, v[timeColI].(string))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		startTime = removeSpanIDFromTime(startTime.UnixNano())
		if spanIDColI == -1 {
			errs = append(errs, errors.New("span_id column not found"))
			continue
		}
		spanID, err := model.SpanIDFromString(v[spanIDColI].(string))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var references []model.SpanRef
		if referencesColI != -1 && v[referencesColI] != nil {
			references, err = referencesFromString(v[referencesColI].(string))
			if err != nil {
				errs = append(errs, errors.WithMessagef(err, "invalid reference '%s'", v[referencesColI].(string)))
				continue
			}
			for i := range references {
				references[i].TraceID = traceID
			}
		}

		if serviceNameColI == -1 {
			errs = append(errs, errors.New("service_name column not found"))
			continue
		}
		process := &model.Process{
			ServiceName: v[serviceNameColI].(string),
		}
		processTagKeys := make(map[string]struct{})
		if processTagKeysColI != -1 && v[processTagKeysColI] != nil {
			for _, k := range strings.Split(v[processTagKeysColI].(string), ",") {
				processTagKeys[k] = struct{}{}
			}
		}
		var tags []model.KeyValue
		for colI, key := range jaegerTagKeysByColI {
			if v[colI] == nil {
				continue
			}
			keyValue, err := stringsToKeyValue(key, v[colI].(string))
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if _, found := processTagKeys[key]; found {
				process.Tags = append(process.Tags, *keyValue)
			} else {
				tags = append(tags, *keyValue)
			}
		}
		var flags model.Flags
		if flagsColI != -1 && v[flagsColI] != nil {
			value, err := v[flagsColI].(json.Number).Int64()
			if err != nil {
				errs = append(errs, err)
				continue
			}
			flags = model.Flags(value)
		}
		var duration time.Duration
		if durationColI != -1 && v[durationColI] != nil {
			value, err := v[durationColI].(json.Number).Int64()
			if err != nil {
				errs = append(errs, err)
				continue
			}
			duration = time.Duration(value)
		}
		if operationNameColI == -1 {
			errs = append(errs, errors.New("operation_name column not found"))
			continue
		}
		operationName := v[operationNameColI].(string)

		span := &model.Span{
			TraceID:       traceID,
			SpanID:        spanID,
			OperationName: operationName,
			References:    references,
			Flags:         flags,
			StartTime:     startTime,
			Duration:      duration,
			Tags:          tags,
			Process:       process,
			Logs:          nil, // Append later, from different measurement
		}

		trace.Spans = append(trace.Spans, span)
	}

	if len(errs) > 0 {
		return nil, multierror.Wrap(errs)
	}

	return &trace, nil
}

// AppendSpanLogsFromInfluxQLResponse appends traces in an InfluxQL response to a slice of traces
func AppendSpanLogsFromInfluxQLResponse(response *client.Response, traces []*model.Trace) error {
	m := make(map[model.TraceID]map[model.SpanID]*model.Span)
	for _, t := range traces {
		m[t.Spans[0].TraceID] = make(map[model.SpanID]*model.Span, len(t.Spans))
		for _, s := range t.Spans {
			m[s.TraceID][s.SpanID] = s
		}
	}

	for _, result := range response.Results {
		for _, row := range result.Series {
			if err := AppendSpanLogsFromInfluxQLRow(&row, m); err != nil {
				return err
			}
		}
	}
	return nil
}

// AppendSpanLogsFromInfluxQLRow appends span logs in an InfluxQL result row to a map of traces/spans
func AppendSpanLogsFromInfluxQLRow(row *models.Row, m map[model.TraceID]map[model.SpanID]*model.Span) error {
	var errs []error

	var traceID model.TraceID
	var err error
	for k, v := range row.Tags {
		if k == common.TraceIDKey {
			traceID, err = model.TraceIDFromString(v)
			if err != nil {
				errs = append(errs, err)
			}
			break
		}
	}
	if traceID.High == 0 && traceID.Low == 0 {
		errs = append(errs, errors.New("trace_id tag not found"))
	}

	timeColI, spanIDColI := -1, -1
	jaegerTagKeysByColI := make(map[int]string, len(row.Columns)-2)

	for colI, col := range row.Columns {
		switch col {
		case common.TimeV1Key:
			timeColI = colI
		case common.SpanIDKey:
			spanIDColI = colI
		default:
			jaegerTagKeysByColI[colI] = col
		}
	}

	for _, v := range row.Values {
		startTime, err := time.Parse(time.RFC3339Nano, v[timeColI].(string))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		spanID, err := model.SpanIDFromString(v[spanIDColI].(string))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		spanLog := model.Log{
			Timestamp: startTime,
		}
		for colI, key := range jaegerTagKeysByColI {
			if v[colI] != nil {
				kv := model.KeyValue{
					Key: key,
				}
				switch vp := v[colI].(type) {
				case string:
					kv.VType = model.ValueType_STRING
					if strings.HasPrefix(vp, "s") {
						kv.VStr = strings.ReplaceAll(vp[1:], "NEWLINE", "\n")
					} else if strings.HasPrefix(vp, "B") {
						b, err := base64.StdEncoding.DecodeString(vp[1:])
						if err != nil {
							errs = append(errs, err)
							continue
						}
						kv.VBinary = b
					} else {
						errs = append(errs, errors.Errorf("proper string prefix not found in log field value for key '%s'", key))
						continue
					}
				case json.Number:
					kv.VType = model.ValueType_FLOAT64
					kv.VFloat64, _ = vp.Float64()
				case bool:
					kv.VType = model.ValueType_BOOL
					kv.VBool = vp
				default:
					errs = append(errs, errors.Errorf("type not supported, key: '%s', type '%s'", key, reflect.TypeOf(v[colI]).String()))
					continue
				}
				spanLog.Fields = append(spanLog.Fields, kv)
			}
		}
		if _, found := m[traceID]; found {
			if s, found := m[traceID][spanID]; found {
				s.Logs = append(s.Logs, spanLog)
			}
		}
	}

	if len(errs) > 0 {
		return multierror.Wrap(errs)
	}

	return nil
}
