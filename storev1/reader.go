package storev1

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/influxdb1-client"
	"github.com/influxdata/jaeger-store/common"
	"github.com/influxdata/jaeger-store/dbmodel"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.uber.org/zap"
)

var _ spanstore.Reader = (*Reader)(nil)

// Reader can query for and load traces from InfluxDB v1.x.
type Reader struct {
	client          *client.Client
	database        string
	retentionPolicy string
	spanMeasurement string
	logMeasurement  string
	defaultLookback time.Duration

	logger *zap.Logger
}

// NewReader returns a new SpanReader for InfluxDB v1.x.
func NewReader(client *client.Client, database, retentionPolicy, spanMeasurement, logMeasurement string, defaultLookback time.Duration, logger *zap.Logger) *Reader {
	return &Reader{
		client:          client,
		database:        database,
		retentionPolicy: retentionPolicy,
		spanMeasurement: spanMeasurement,
		logMeasurement:  logMeasurement,
		defaultLookback: defaultLookback,
		logger:          logger,
	}
}

func (r *Reader) query(ctx context.Context, influxQLQuery string) (*client.Response, error) {
	println(influxQLQuery)
	q := client.Query{
		Command:         influxQLQuery,
		Database:        r.database,
		RetentionPolicy: r.retentionPolicy,
	}

	response, err := r.client.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	if response.Err != nil {
		return nil, response.Err
	}

	return response, nil
}

var queryGetServicesInfluxQL = fmt.Sprintf(`show tag values with key = "%s"`, common.ServiceNameKey)

// GetServices returns all services traced by Jaeger
func (r *Reader) GetServices(ctx context.Context) ([]string, error) {
	response, err := r.query(ctx, queryGetServicesInfluxQL)
	if err != nil {
		return nil, err
	}

	var services []string
	for _, result := range response.Results {
		if result.Err != nil {
			return nil, result.Err
		}
		for _, row := range result.Series {
			valueColI := -1
			for i, c := range row.Columns {
				if c == "value" {
					valueColI = i
					break
				}
			}
			for _, v := range row.Values {
				services = append(services, v[valueColI].(string))
			}
		}
	}

	return services, nil
}

var queryGetOperationsInfluxQL = fmt.Sprintf(`show tag values with key = "%s" where "%s" = '%%s'`, common.OperationNameKey, common.ServiceNameKey)

// GetOperations returns all operations for a specific service traced by Jaeger
func (r *Reader) GetOperations(ctx context.Context, service string) ([]string, error) {
	response, err := r.query(ctx, fmt.Sprintf(queryGetOperationsInfluxQL, service))
	if err != nil {
		return nil, err
	}

	var operations []string
	for _, result := range response.Results {
		if result.Err != nil {
			return nil, result.Err
		}
		for _, row := range result.Series {
			valueColI := -1
			for i, c := range row.Columns {
				if c == "value" {
					valueColI = i
					break
				}
			}
			for _, v := range row.Values {
				operations = append(operations, v[valueColI].(string))
			}
		}
	}

	return operations, nil
}

// FindTraces retrieve traces that match the traceQuery
func (r *Reader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	traceIDs, err := r.FindTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(traceIDs) == 0 {
		return nil, nil
	}

	var traces []*model.Trace
	var tracesErr error
	var tracesWG sync.WaitGroup // Do not read traces or tracesErr until tracesWG.Wait() returns
	tracesWG.Add(1)

	go func() {
		defer tracesWG.Done()

		response, err := r.query(ctx, dbmodel.NewInfluxQLTraceQuery(r.spanMeasurement).BuildTraceQuery(traceIDs))
		if err != nil {
			tracesErr = err
			return
		}

		traces, err = dbmodel.TracesFromInfluxQLResponse(response)
		if err != nil {
			tracesErr = err
			return
		}
	}()

	response, err := r.query(ctx, dbmodel.NewInfluxQLLogQuery(r.logMeasurement).BuildLogQuery(traceIDs))
	if err != nil {
		return nil, err
	}

	tracesWG.Wait()
	if tracesErr != nil {
		return nil, tracesErr
	}

	err = dbmodel.AppendSpanLogsFromInfluxQLResponse(response, traces)
	if err != nil {
		return nil, err
	}

	return traces, nil
}

// GetTrace takes a traceID and returns a Trace associated with that traceID
func (r *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	println("GetTrace called")

	response, err := r.query(ctx, dbmodel.NewInfluxQLTraceQuery(r.spanMeasurement).BuildTraceQuery([]model.TraceID{traceID}))
	if err != nil {
		return nil, err
	}

	traces, err := dbmodel.TracesFromInfluxQLResponse(response)
	if err != nil {
		return nil, err
	}
	if len(traces) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}
	if len(traces) > 1 {
		panic("more than one trace returned, expected exactly one; bug in query?")
	}

	response, err = r.query(ctx, dbmodel.NewInfluxQLLogQuery(r.logMeasurement).BuildLogQuery([]model.TraceID{traceID}))
	if err != nil {
		return nil, err
	}

	err = dbmodel.AppendSpanLogsFromInfluxQLResponse(response, traces)
	if err != nil {
		return nil, err
	}

	return traces[0], nil
}

// FindTraceIDs retrieve traceIDs that match the traceQuery
func (r *Reader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	response, err := r.query(ctx, dbmodel.InfluxQLTraceQueryFromTQP(r.spanMeasurement, query).BuildTraceIDQuery())
	if err != nil {
		return nil, err
	}

	return dbmodel.TraceIDsFromInfluxQLResult(response)
}

// We don't use duration, just need span_id,service_name where references is null.
const getDependenciesQueryInfluxQL = `select "span_id", "service_name", "references", "duration" from %s where time > now() - %s`

// GetDependencies returns all inter-service dependencies
func (r *Reader) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	response, err := r.query(context.Background(), fmt.Sprintf(getDependenciesQueryInfluxQL, r.spanMeasurement, lookback.String()))
	if err != nil {
		return nil, err
	}

	return dbmodel.DependencyLinksFromResultV1(response)
}
