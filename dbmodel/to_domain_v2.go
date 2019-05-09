package dbmodel

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/jaeger-store/common"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/multierror"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// spanLogFromFluxColReader converts a Flux column reader (query response "row") to a Jaeger span log
func spanLogFromFluxColReader(reader flux.ColReader, rowI int, logger *zap.Logger) (*model.Log, model.TraceID, model.SpanID, error) {
	var log model.Log
	var traceID model.TraceID
	var spanID model.SpanID
	var err error
	var errs []error

	for colI, col := range reader.Cols() {
		if isNull(reader, colI, rowI) {
			continue
		}

		switch col.Label {
		case common.MeasurementKey:
			continue

		case common.TimeV2Key:
			log.Timestamp = time.Unix(0, reader.Times(colI).Value(rowI))
		case common.TraceIDKey:
			traceID, err = model.TraceIDFromString(reader.Strings(colI).ValueString(rowI))
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to parse trace ID '%s' in span log", reader.Strings(colI).ValueString(rowI)))
				continue
			}
		case common.SpanIDKey:
			spanID, err = model.SpanIDFromString(reader.Strings(colI).ValueString(rowI))
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to parse span ID '%s' in span log", reader.Strings(colI).ValueString(rowI)))
				continue
			}
		default:
			kv := model.KeyValue{
				Key: col.Label,
			}
			switch col.Type {
			case flux.TBool:
				kv.VType = model.ValueType_BOOL
				kv.VBool = reader.Bools(colI).Value(rowI)
			case flux.TInt:
				kv.VType = model.ValueType_INT64
				kv.VInt64 = reader.Ints(colI).Value(rowI)
			case flux.TUInt:
				kv.VType = model.ValueType_INT64
				kv.VInt64 = int64(reader.UInts(colI).Value(rowI))
			case flux.TFloat:
				kv.VType = model.ValueType_FLOAT64
				kv.VFloat64 = reader.Floats(colI).Value(rowI)
			case flux.TString:
				s := reader.Strings(colI).ValueString(rowI)
				if strings.HasPrefix(s, "s") {
					kv.VType = model.ValueType_STRING
					kv.VStr = strings.ReplaceAll(s[1:], "NEWLINE", "\n")
				} else if strings.HasPrefix(s, "B") {
					kv.VType = model.ValueType_BINARY
					b, err := base64.StdEncoding.DecodeString(s[1:])
					if err != nil {
						errs = append(errs, err)
						continue
					}
					kv.VBinary = b
				} else {
					errs = append(errs, errors.Errorf("proper string prefix not found in log field value for key '%s'", col.Label))
					continue
				}
			case flux.TTime:
				kv.VType = model.ValueType_INT64
				kv.VInt64 = int64(time.Unix(0, reader.Times(colI).Value(rowI)).Nanosecond())
			default:
				logger.Info("skipping span log field with unrecognized type", zap.String("key", col.Label))
				continue
			}
			log.Fields = append(log.Fields, kv)
		}
	}

	if log.Timestamp.IsZero() {
		errs = append(errs, errors.New("log timestamp not found in flux fields"))
	}
	if traceID.Low == 0 && traceID.High == 0 {
		errs = append(errs, errors.New("log trace ID not found in flux fields"))
	}
	if spanID == 0 {
		errs = append(errs, errors.New("log span ID not found in flux fields"))
	}

	if len(errs) > 0 {
		return nil, model.TraceID{}, 0, multierror.Wrap(errs)
	}

	return &log, traceID, spanID, nil
}

// SpanFromFluxColReader converts a flux Reader row to a Jaeger span.
func SpanFromFluxColReader(reader flux.ColReader, rowI int) (*model.Span, error) {
	span := model.Span{
		Process: &model.Process{},
	}
	processTagKeys := make(map[string]struct{})
	var errs []error

	for colI, col := range reader.Cols() {
		if isNull(reader, colI, rowI) {
			continue
		}

		switch col.Label {
		case common.MeasurementKey:
			continue

		case common.TimeV2Key:
			span.StartTime = removeSpanIDFromTime(reader.Times(colI).Value(rowI))

		case common.TraceIDKey:
			var err error
			span.TraceID, err = model.TraceIDFromString(reader.Strings(colI).ValueString(rowI))
			if err != nil {
				errs = append(errs, err)
				continue
			}

		case common.ServiceNameKey:
			span.Process.ServiceName = reader.Strings(colI).ValueString(rowI)

		case common.OperationNameKey:
			span.OperationName = reader.Strings(colI).ValueString(rowI)

		case common.SpanIDKey:
			var err error
			span.SpanID, err = model.SpanIDFromString(reader.Strings(colI).ValueString(rowI))
			if err != nil {
				errs = append(errs, err)
				continue
			}

		case common.DurationKey:
			span.Duration = time.Duration(reader.Ints(colI).Value(rowI))

		case common.FlagsKey:
			span.Flags = model.Flags(reader.Ints(colI).Value(rowI))

		case common.ProcessTagKeysKey:
			for _, k := range strings.Split(reader.Strings(colI).ValueString(rowI), ",") {
				processTagKeys[k] = struct{}{}
			}

		case common.ReferencesKey:
			references, err := referencesFromString(reader.Strings(colI).ValueString(rowI))
			if err != nil {
				errs = append(errs, errors.WithMessagef(err, "invalid reference '%s'", reader.Strings(colI).ValueString(rowI)))
				continue
			}
			span.References = references

		default:
			parts := strings.SplitN(col.Label, ":", 2)
			if len(parts) < 2 {
				errs = append(errs, fmt.Errorf("unrecognized field key '%s'", col.Label))
				continue
			}
			prefix, key := parts[0], parts[1]

			switch prefix {
			case common.TagKeyPrefix:
				// Assume this is a span tag, which means the value is string type.
				tag, err := stringsToKeyValue(key, reader.Strings(colI).ValueString(rowI))
				if err != nil {
					errs = append(errs, errors.WithMessagef(err, "invalid tag value '%s'", reader.Strings(colI).ValueString(rowI)))
					continue
				}
				span.Tags = append(span.Tags, *tag)

			default:
				errs = append(errs, fmt.Errorf("unrecognized field key prefix '%s'", col.Label))
			}
		}
	}

	for i := range span.References {
		span.References[i].TraceID = span.TraceID
	}

	for i := len(span.Tags) - 1; i >= 0; i-- {
		if _, found := processTagKeys[span.Tags[i].Key]; found {
			// Move tag from span to span.Process
			span.Process.Tags = append(span.Process.Tags, span.Tags[i])
			span.Tags[i] = span.Tags[len(span.Tags)-1]
			span.Tags = span.Tags[:len(span.Tags)-1]
		}
	}

	if len(errs) > 0 {
		return nil, multierror.Wrap(errs)
	}

	return &span, nil
}

func isNull(reader flux.ColReader, colI, rowI int) bool {
	switch reader.Cols()[colI].Type {
	case flux.TBool:
		return reader.Bools(colI).IsNull(rowI)
	case flux.TInt:
		return reader.Ints(colI).IsNull(rowI)
	case flux.TUInt:
		return reader.UInts(colI).IsNull(rowI)
	case flux.TFloat:
		return reader.Floats(colI).IsNull(rowI)
	case flux.TString:
		return reader.Strings(colI).IsNull(rowI)
	case flux.TTime:
		return reader.Times(colI).IsNull(rowI)
	default:
		panic("unreachable")
	}
}

func getMeasurement(table flux.Table) (string, error) {
	measurement := table.Key().LabelValue(common.MeasurementKey)
	if measurement == nil {
		return "", errors.Errorf("query result does not contain '%s' column", common.MeasurementKey)
	}
	return measurement.Str(), nil
}

// traceFromFluxTable converts a flux Table to a Jaeger trace.
func traceFromFluxTable(table flux.Table) (*model.Trace, error) {
	var trace model.Trace
	err := table.Do(func(reader flux.ColReader) error {
		for rowI := 0; rowI < reader.Len(); rowI++ {
			span, err := SpanFromFluxColReader(reader, rowI)
			if err != nil {
				return err
			}
			trace.Spans = append(trace.Spans, span)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &trace, nil
}

// TracesFromFluxResult converts a flux Result to Jaeger traces.
func TracesFromFluxResult(result flux.Result, spanMeasurement, logMeasurement string, logger *zap.Logger) ([]*model.Trace, error) {
	var traces []*model.Trace
	logsByTraceIDSpanID := make(map[model.TraceID]map[model.SpanID][]model.Log)

	err := result.Tables().Do(func(table flux.Table) error {
		measurement, err := getMeasurement(table)
		if err != nil {
			return err
		}
		switch measurement {
		case spanMeasurement:
			trace, err := traceFromFluxTable(table)
			if err == nil {
				traces = append(traces, trace)
			}
			return err

		case logMeasurement:
			var traceID model.TraceID
			if v := table.Key().LabelValue(common.TraceIDKey); v == nil {
				return errors.Errorf("column '%s' not found in log table key", common.TraceIDKey)
			} else {
				traceID, err = model.TraceIDFromString(v.Str())
				if err != nil {
					return errors.WithMessage(err, "failed to deserialize '%s' from log table key")
				}
			}

			logs, err := logsFromFluxTable(table, logger)
			if err == nil {
				logsByTraceIDSpanID[traceID] = logs
			}
			return err

		default:
			return errors.Errorf("don't know what to do with measurement '%s'", measurement)
		}
	})
	if err != nil {
		return nil, err
	}

	for _, trace := range traces {
		for _, span := range trace.Spans {
			if logsBySpanID := logsByTraceIDSpanID[span.TraceID]; logsBySpanID != nil {
				span.Logs = logsBySpanID[span.SpanID]
			}
		}
	}

	return traces, nil
}

func logsFromFluxTable(table flux.Table, logger *zap.Logger) (map[model.SpanID][]model.Log, error) {
	logsBySpanID := make(map[model.SpanID][]model.Log)
	var err error

	err = table.Do(func(reader flux.ColReader) error {
		for rowI := 0; rowI < reader.Len(); rowI++ {
			spanLog, _, spanID, err := spanLogFromFluxColReader(reader, rowI, logger)
			if err != nil {
				logger.Info("failed to get span log from flux result", zap.Error(err))
				continue
			}
			logsBySpanID[spanID] = append(logsBySpanID[spanID], *spanLog)
		}
		return nil
	})
	if err != nil || len(logsBySpanID) < 1 {
		return nil, err
	}

	return logsBySpanID, nil
}

// TraceIDsFromFluxResult converts an InfluxDB result to a slice of TraceIDs
func TraceIDsFromFluxResult(result flux.Result) ([]model.TraceID, error) {
	var traceIDs []model.TraceID

	err := result.Tables().Do(func(table flux.Table) error {
		return table.Do(func(reader flux.ColReader) error {
			var colI = -1
			for i := range reader.Cols() {
				if reader.Cols()[i].Label == common.TraceIDKey {
					colI = i
				}
			}
			if colI == -1 {
				return fmt.Errorf("column '%s' not found in Flux result", common.TraceIDKey)
			}
			for rowI := 0; rowI < reader.Len(); rowI++ {
				traceID, err := model.TraceIDFromString(reader.Strings(colI).ValueString(rowI))
				if err != nil {
					return errors.WithMessagef(err, "failed to get traceID from flux value '%s'", reader.Strings(colI).ValueString(rowI))
				}
				traceIDs = append(traceIDs, traceID)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return traceIDs, nil
}

func stringsToKeyValue(k, v string) (*model.KeyValue, error) {
	if v == "" {
		return nil, nil
	}

	kv := model.KeyValue{
		Key: k,
	}

	parts := strings.SplitN(v, ":", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid kv value '%s'", v)
	}

	ttype, strValue := parts[0], parts[1]

	switch ttype {
	case "s":
		kv.VType = model.ValueType_STRING
		kv.VStr = strValue

	case "b":
		kv.VType = model.ValueType_BOOL
		switch strValue {
		case "t":
			kv.VBool = true
		case "f":
			kv.VBool = false
		default:
			return nil, fmt.Errorf("invalid boolean kv value '%s'", strValue)
		}

	case "i":
		var err error
		kv.VType = model.ValueType_INT64
		kv.VInt64, err = strconv.ParseInt(strValue, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer kv value '%s'", strValue)
		}

	case "f":
		var err error
		kv.VType = model.ValueType_FLOAT64
		kv.VFloat64, err = strconv.ParseFloat(strValue, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float kv value '%s'", strValue)
		}

	case "B":
		kv.VType = model.ValueType_BINARY
		var err error
		kv.VBinary, err = base64.StdEncoding.DecodeString(strValue)
		if err != nil {
			return nil, fmt.Errorf("invalid binary kv value '%s'", strValue)
		}

	default:
		return nil, fmt.Errorf("invalid kv type '%s'", ttype)
	}

	return &kv, nil
}

// DependencyLinksFromResultV2 converts an InfluxDB result to a dependency graph
func DependencyLinksFromResultV2(result flux.Result) ([]model.DependencyLink, error) {
	parentByChild := make(map[model.SpanID]model.SpanID)
	serviceNameBySpanID := make(map[model.SpanID]string)

	err := result.Tables().Do(func(table flux.Table) error {
		return table.Do(func(reader flux.ColReader) error {
			spanIDColI, serviceNameColI, referencesColI := -1, -1, -1
			for i := range reader.Cols() {
				switch reader.Cols()[i].Label {
				case common.SpanIDKey:
					spanIDColI = i
				case common.ServiceNameKey:
					serviceNameColI = i
				case common.ReferencesKey:
					referencesColI = i
				}
			}

			for rowI := 0; rowI < reader.Len(); rowI++ {
				spanID, err := model.SpanIDFromString(reader.Strings(spanIDColI).ValueString(rowI))
				if err != nil {
					return errors.WithMessagef(err, "failed to parse SpanID '%s'", reader.Strings(spanIDColI).ValueString(rowI))
				}
				serviceName := reader.Strings(serviceNameColI).ValueString(rowI)
				serviceNameBySpanID[spanID] = serviceName

				if referencesColI > -1 {
					references, err := referencesFromString(reader.Strings(referencesColI).ValueString(rowI))
					if err != nil {
						return errors.WithMessagef(err, "failed to parse references '%s'", reader.Strings(referencesColI).ValueString(rowI))
					}
					for _, reference := range references {
						if reference.RefType == model.SpanRefType_CHILD_OF {
							parentByChild[spanID] = reference.SpanID
						}
					}
				}
			}

			return nil
		})
	})
	if err != nil {
		return nil, err
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
