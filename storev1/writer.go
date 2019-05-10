package storev1

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/influxdata/influxdb1-client"
	"github.com/influxdata/jaeger-influxdb/common"
	"github.com/influxdata/jaeger-influxdb/dbmodel"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.uber.org/zap"
)

var (
	_ spanstore.Writer = (*Writer)(nil)
	_ io.Closer        = (*Writer)(nil)
)

// Writer handles all writes to InfluxDB v1.x for the Jaeger data model
type Writer struct {
	client          *client.Client
	database        string
	retentionPolicy string
	spanMeasurement string
	logMeasurement  string

	// Points as line protocol
	writeCh chan string
	writeWG sync.WaitGroup

	logger *zap.Logger
}

// NewWriter returns a Writer for InfluxDB v1.x
func NewWriter(client *client.Client, database, retentionPolicy, spanMeasurement, logMeasurement string, logger *zap.Logger) *Writer {
	w := &Writer{
		client:          client,
		database:        database,
		retentionPolicy: retentionPolicy,
		spanMeasurement: spanMeasurement,
		logMeasurement:  logMeasurement,

		writeCh: make(chan string),

		logger: logger,
	}

	w.writeWG.Add(1)

	go func() {
		w.batchAndWrite()
		w.writeWG.Done()
	}()

	return w
}

// Close triggers a graceful shutdown
func (w *Writer) Close() error {
	close(w.writeCh)
	w.writeWG.Wait()
	return nil
}

// WriteSpan saves the span into Cassandra
func (w *Writer) WriteSpan(span *model.Span) error {
	points, err := dbmodel.SpanToPointsV1(span, w.spanMeasurement, w.logMeasurement, w.logger)
	if err != nil {
		return err
	}

	for _, point := range points {
		w.writeCh <- point.String()
	}
	return nil
}

func (w *Writer) batchAndWrite() {
	batch := make([]string, 0, common.MaxFlushPoints)
	var t <-chan time.Time

	for {
		select {
		case point, ok := <-w.writeCh:
			if !ok {
				if len(batch) > 0 {
					w.writeBatch(batch)
					return
				}
			}

			if t == nil {
				t = time.After(common.MaxFlushInterval)
			}

			batch = append(batch, point)

			if len(batch) == cap(batch) {
				w.writeBatch(batch)
				batch = batch[:0]
				t = nil
			}

		case <-t:
			w.writeBatch(batch)
			batch = batch[:0]
			t = nil
		}
	}
}

func (w *Writer) writeBatch(batch []string) {
	buf := bytes.NewBuffer([]byte{})
	for _, point := range batch {
		_, _ = buf.WriteString(point)
		_, _ = buf.WriteRune('\n')
	}

	response, err := w.client.WriteLineProtocol(buf.String(), w.database, w.retentionPolicy, "ns", "any")
	if err != nil {
		w.logger.Warn("failed to write batch", zap.Error(err))
		return
	}
	if response != nil && response.Err != nil {
		w.logger.Warn("failed to write batch", zap.Error(response.Err))
		return
	}
	w.logger.Debug("wrote points to InfluxDB", zap.Int("quantity", len(batch)))
}
