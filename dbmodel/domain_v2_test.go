package dbmodel_test

import (
	gojson "encoding/json"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/jaeger-store/common"
	"github.com/influxdata/jaeger-store/dbmodel"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/model/converter/json"
	"go.uber.org/zap"
)

const (
	spanMeasurement = "span"
	logMeasurement  = "log"
)

var (
	traceID = model.NewTraceID(1234567890123456789, 234567890123456789)
	spanA   = model.Span{
		TraceID:       traceID,
		SpanID:        model.NewSpanID(34567890123456789),
		OperationName: "test-operation-a",
		References:    nil, // root span has no references
		Flags:         model.DebugFlag,
		StartTime:     time.Unix(1550013480, 123000),
		Duration:      time.Duration(12345 * time.Microsecond),
		Tags: []model.KeyValue{
			{
				Key:   "tag-a",
				VType: model.ValueType_STRING,
				VStr:  "value-a",
			},
			{
				Key:   "tag-b",
				VType: model.ValueType_BOOL,
				VBool: true,
			},
			{
				Key:    "tag-c",
				VType:  model.ValueType_INT64,
				VInt64: 123,
			},
			{
				Key:      "tag-d",
				VType:    model.ValueType_FLOAT64,
				VFloat64: 123.456,
			},
			{
				Key:     "tag-e",
				VType:   model.ValueType_BINARY,
				VBinary: []byte("foo"),
			},
		},
		Logs: []model.Log{
			{
				Timestamp: time.Unix(0, 1550013480000300000),
				Fields: []model.KeyValue{
					{
						Key:   "k",
						VType: model.ValueType_STRING,
						VStr:  "v",
					},
				},
			},
			{
				Timestamp: time.Unix(0, 1550013480000301000),
				Fields: []model.KeyValue{
					{
						Key:   "have newline",
						VType: model.ValueType_STRING,
						VStr:  "foo\nbar",
					},
				},
			},
			{
				Timestamp: time.Unix(0, 1550013480000302000),
				Fields: []model.KeyValue{
					{
						Key:     "is binary",
						VType:   model.ValueType_BINARY,
						VBinary: []byte("foo bar"),
					},
				},
			},
		},
		Process: model.NewProcess(
			"test-service-a",
			[]model.KeyValue{
				{
					Key:   "process-tag-a",
					VType: model.ValueType_STRING,
					VStr:  "process-value-a",
				},
			}),
		ProcessID: "",  // Unused
		Warnings:  nil, // Unused
	}

	spanB = model.Span{
		TraceID:       traceID,
		SpanID:        model.NewSpanID(543212345),
		OperationName: "test-operation-b",
		References: []model.SpanRef{
			{
				TraceID: traceID,
				SpanID:  spanA.SpanID,
				RefType: model.SpanRefType_CHILD_OF,
			},
		},
		Flags:     model.SampledFlag,
		StartTime: time.Unix(1550013480, 200000),
		Duration:  time.Duration(1234 * time.Microsecond),
		Tags: []model.KeyValue{
			{
				Key:   "other-tag-a",
				VType: model.ValueType_STRING,
				VStr:  "other-value-a",
			},
		},
		Process: model.NewProcess(
			"test-service-a",
			[]model.KeyValue{
				{
					Key:   "other-process-tag-a",
					VType: model.ValueType_STRING,
					VStr:  "other-process-value-a",
				},
			}),
	}

	// points are the InfluxDB write objects.
	pointA0, _ = models.NewPoint(spanMeasurement,
		models.NewTags(
			map[string]string{
				common.TraceIDKey:       traceID.String(),
				common.ServiceNameKey:   spanA.Process.ServiceName,
				common.OperationNameKey: spanA.OperationName,
				"tag:tag-a":             "s:value-a",
				"tag:tag-b":             "b:t",
				"tag:tag-c":             "i:123",
				"tag:tag-d":             "f:" + strconv.FormatFloat(123.456, 'E', -1, 64),
				"tag:tag-e":             "B:Zm9v",
				"tag:process-tag-a":     "s:process-value-a",
			}),
		map[string]interface{}{
			common.SpanIDKey:         spanA.SpanID.String(),
			common.DurationKey:       spanA.Duration.Nanoseconds(),
			common.FlagsKey:          uint32(spanA.Flags),
			common.ProcessTagKeysKey: "process-tag-a",
		},
		time.Unix(0, 1550013480000123726)) // 726 is the timestamp hash suffix, not recognized by Jaeger
	pointA1, _ = models.NewPoint(logMeasurement,
		models.NewTags(map[string]string{
			common.TraceIDKey: traceID.String(),
		}),
		map[string]interface{}{
			common.SpanIDKey: spanA.SpanID.String(),
			"k":              "sv",
		},
		time.Unix(0, 1550013480000300000))
	pointA2, _ = models.NewPoint(logMeasurement,
		models.NewTags(map[string]string{
			common.TraceIDKey: traceID.String(),
		}),
		map[string]interface{}{
			common.SpanIDKey: spanA.SpanID.String(),
			"have newline":   "sfooNEWLINEbar",
		},
		time.Unix(0, 1550013480000301000))
	pointA3, _ = models.NewPoint(logMeasurement,
		models.NewTags(map[string]string{
			common.TraceIDKey: traceID.String(),
		}),
		map[string]interface{}{
			common.SpanIDKey: spanA.SpanID.String(),
			"is binary":      "BZm9vIGJhcg==",
		},
		time.Unix(0, 1550013480000302000))
	pointsA = models.Points{pointA0, pointA1, pointA2, pointA3}

	pointB0, _ = models.NewPoint(spanMeasurement,
		models.NewTags(
			map[string]string{
				common.TraceIDKey:         traceID.String(),
				common.ServiceNameKey:     spanB.Process.ServiceName,
				common.OperationNameKey:   spanB.OperationName,
				"tag:other-tag-a":         "s:other-value-a",
				"tag:other-process-tag-a": "s:other-process-value-a",
			}),
		map[string]interface{}{
			common.SpanIDKey:         spanB.SpanID.String(),
			common.DurationKey:       spanB.Duration.Nanoseconds(),
			common.FlagsKey:          uint32(spanB.Flags),
			common.ProcessTagKeysKey: "other-process-tag-a",
			common.ReferencesKey:     "7acf501b718115:ChildOf",
		},
		time.Unix(0, 1550013480000200883)) // 883 is the timestamp hash suffix, not recognized by Jaeger
	pointsB = models.Points{pointB0}

	// tables are Flux query result tables.
	spanTable = executetest.Table{
		GroupKey: execute.NewGroupKey(
			[]flux.ColMeta{{
				Label: common.MeasurementKey,
				Type:  flux.TString,
			}, {
				Label: common.TraceIDKey,
				Type:  flux.TString,
			}},
			[]values.Value{
				values.New("span"),
				values.New(traceID.String()),
			}),
		KeyCols: []string{common.TraceIDKey},
		ColMeta: []flux.ColMeta{
			{Label: common.TimeV2Key, Type: flux.TTime},
			{Label: common.TraceIDKey, Type: flux.TString},
			{Label: common.SpanIDKey, Type: flux.TString},
			{Label: common.ServiceNameKey, Type: flux.TString},
			{Label: common.OperationNameKey, Type: flux.TString},
			{Label: "tag:tag-a", Type: flux.TString},
			{Label: "tag:tag-b", Type: flux.TString},
			{Label: "tag:tag-c", Type: flux.TString},
			{Label: "tag:tag-d", Type: flux.TString},
			{Label: "tag:tag-e", Type: flux.TString},
			{Label: "tag:other-tag-a", Type: flux.TString},
			{Label: "tag:process-tag-a", Type: flux.TString},
			{Label: "tag:other-process-tag-a", Type: flux.TString},
			{Label: common.DurationKey, Type: flux.TInt},
			{Label: common.FlagsKey, Type: flux.TInt},
			{Label: common.ProcessTagKeysKey, Type: flux.TString},
			{Label: common.ReferencesKey, Type: flux.TString},
		},
		Data: [][]interface{}{
			{
				values.ConvertTime(time.Unix(0, 1550013480000123000)),
				traceID.String(),
				spanA.SpanID.String(),
				spanA.Process.ServiceName,
				spanA.OperationName,
				"s:value-a",
				"b:t",
				"i:123",
				"f:" + strconv.FormatFloat(123.456, 'E', -1, 64),
				"B:Zm9v",
				nil,
				"s:process-value-a",
				nil,
				spanA.Duration.Nanoseconds(),
				int64(spanA.Flags),
				"process-tag-a",
				nil,
			},
			{
				values.ConvertTime(time.Unix(0, 1550013480000200000)),
				traceID.String(),
				spanB.SpanID.String(),
				spanB.Process.ServiceName,
				spanB.OperationName,
				nil,
				nil,
				nil,
				nil,
				nil,
				"s:other-value-a",
				nil,
				"s:other-process-value-a",
				spanB.Duration.Nanoseconds(),
				int64(spanB.Flags),
				"other-process-tag-a",
				"7acf501b718115:ChildOf",
			},
		},
	}
	logTable = executetest.Table{
		GroupKey: execute.NewGroupKey(
			[]flux.ColMeta{{
				Label: common.MeasurementKey,
				Type:  flux.TString,
			}, {
				Label: common.TraceIDKey,
				Type:  flux.TString,
			}},
			[]values.Value{
				values.New("log"),
				values.New(traceID.String()),
			}),
		KeyCols: []string{common.TraceIDKey},
		ColMeta: []flux.ColMeta{
			{Label: common.TimeV2Key, Type: flux.TTime},
			{Label: common.TraceIDKey, Type: flux.TString},
			{Label: common.SpanIDKey, Type: flux.TString},
			{Label: "k", Type: flux.TString},
			{Label: "have newline", Type: flux.TString},
			{Label: "is binary", Type: flux.TString},
		},
		Data: [][]interface{}{
			{
				values.ConvertTime(time.Unix(0, 1550013480000300000)),
				traceID.String(),
				spanA.SpanID.String(),
				"sv",
				nil,
				nil,
			},
			{
				values.ConvertTime(time.Unix(0, 1550013480000301000)),
				traceID.String(),
				spanA.SpanID.String(),
				nil,
				"sfooNEWLINEbar",
				nil,
			},
			{
				values.ConvertTime(time.Unix(0, 1550013480000302000)),
				traceID.String(),
				spanA.SpanID.String(),
				nil,
				nil,
				"BZm9vIGJhcg==",
			},
		},
	}
)

func TestSpanToPointV2(t *testing.T) {
	gotPointsA, err := dbmodel.SpanToPointsV2(&spanA, spanMeasurement, logMeasurement, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	requirePointssEqual(t, pointsA, gotPointsA, "points are not equal")

	gotPointsB, err := dbmodel.SpanToPointsV2(&spanB, spanMeasurement, logMeasurement, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	requirePointssEqual(t, pointsB, gotPointsB, "points are not equal")

	if spanB.References[0].SpanID != spanA.SpanID {
		t.Errorf("referenced parent span ID does not match parent span: %d vs %d", spanA.SpanID, spanB.References[0].SpanID)
	}
}

func requirePointssEqual(t *testing.T, a, b models.Points, errMessage string) {
	var aStrings []string
	for _, p := range a {
		aStrings = append(aStrings, p.String())
	}
	sort.Strings(aStrings)

	var bStrings []string
	for _, p := range b {
		bStrings = append(bStrings, p.String())
	}
	sort.Strings(bStrings)

	l := len(a)
	if len(b) < len(a) {
		l = len(b)
	}

	for i := 0; i < l; i++ {
		requireStringsEqual(t, aStrings[i], bStrings[i], errMessage)
	}

	if len(a) != len(b) {
		t.Errorf("expected %d points but got %d", len(a), len(b))
	}
}

func requireStringsEqual(t *testing.T, a, b, errMessage string) {
	if a != b {
		i := findStringDiffI(a, b)
		t.Errorf("%s:\nexpected:  '%s'\ngot:       '%s'\nat:         %s^", errMessage, a, b, strings.Repeat(" ", i))
	}
}

func findStringDiffI(a, b string) int {
	l := len(a)
	if len(b) < len(a) {
		l = len(b)
	}
	for i := 0; i < l; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return -1
}

func sortTraceFields(t *model.Trace) {
	sort.Slice(t.Spans, func(i, j int) bool {
		return t.Spans[i].SpanID < t.Spans[j].SpanID
	})
	for _, span := range t.Spans {
		sort.Slice(span.References, func(i, j int) bool {
			return span.References[i].SpanID < span.References[j].SpanID
		})
		sort.Slice(span.Tags, func(i, j int) bool {
			return span.Tags[i].Key < span.Tags[j].Key
		})
		sort.Slice(span.Logs, func(i, j int) bool {
			return span.Logs[i].Timestamp.UnixNano() < span.Logs[j].Timestamp.UnixNano()
		})
		for k := range span.Logs {
			sort.Slice(span.Logs[k].Fields, func(i, j int) bool {
				return span.Logs[k].Fields[i].Key < span.Logs[k].Fields[j].Key
			})
		}
		sort.Slice(span.Warnings, func(i, j int) bool {
			return span.Warnings[i] < span.Warnings[j]
		})
		sort.Slice(span.Process.Tags, func(i, j int) bool {
			return span.Process.Tags[i].Key < span.Process.Tags[j].Key
		})
	}
}

func TestSpanFromFlux(t *testing.T) {
	expectedTrace := &model.Trace{
		Spans: []*model.Span{&spanA, &spanB},
	}
	sortTraceFields(expectedTrace)

	fluxResult := &executetest.Result{
		Tbls: []*executetest.Table{&spanTable, &logTable},
	}
	traces, err := dbmodel.TracesFromFluxResult(fluxResult, spanMeasurement, logMeasurement, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) != 1 {
		t.Fatalf("expected 1 trace but got %d traces", len(traces))
	}
	gotTrace := traces[0]
	sortTraceFields(gotTrace)

	expectedJSON, err := gojson.Marshal(json.FromDomain(expectedTrace))
	if err != nil {
		t.Fatal(err)
	}
	gotJSON, err := gojson.Marshal(json.FromDomain(gotTrace))
	if err != nil {
		t.Fatal(err)
	}

	requireStringsEqual(t, string(expectedJSON), string(gotJSON), "traces not equal")
}

func TestSpanToPointV2_invalidReference(t *testing.T) {
	// When Jaeger sends a trace with multiple spans, the root span contains this reference.
	// We should treat it as if the reference didn't exist.
	span := spanA
	span.References = []model.SpanRef{
		{
			TraceID: span.TraceID,
			SpanID:  0,
			RefType: model.SpanRefType_CHILD_OF,
		},
	}
	points, err := dbmodel.SpanToPointsV2(&span, spanMeasurement, logMeasurement, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 4 {
		t.Fatalf("expected 2 points, got %d", len(points))
	}
	point := points[0]

	fields, err := point.Fields()
	if err != nil {
		t.Fatal(err)
	}
	if references, found := fields[common.ReferencesKey]; found {
		t.Errorf("expected root span to contain zero references, but got '%s'", references)
	}
}
