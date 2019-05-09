package storev1

import (
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/influxdata/influxdb1-client"
	"github.com/influxdata/jaeger-store/common"
	"github.com/influxdata/jaeger-store/config"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.uber.org/zap"
)

var (
	_ shared.StoragePlugin = (*Store)(nil)
	_ io.Closer            = (*Store)(nil)
)

type Store struct {
	reader *Reader
	writer *Writer
}

func NewStore(conf *config.Configuration, logger *zap.Logger) (*Store, error) {
	u, err := url.ParseRequestURI(conf.Host)
	if err != nil {
		return nil, err
	}

	clientConfig := client.Config{
		URL:       *u,
		Username:  conf.Username,
		Password:  conf.Password,
		Timeout:   5 * time.Second,
		UnsafeSsl: conf.UnsafeSsl,
		UserAgent: fmt.Sprintf("jaeger-influxdb"),
	}

	influxClient, err := client.NewClient(clientConfig)
	if err != nil {
		return nil, err
	}

	reader := NewReader(influxClient, conf.Database, conf.RetentionPolicy, common.DefaultSpanMeasurement, common.DefaultLogMeasurement, conf.DefaultLookback, logger)

	writer := NewWriter(influxClient, conf.Database, conf.RetentionPolicy, common.DefaultSpanMeasurement, common.DefaultLogMeasurement, logger)

	return &Store{
		reader: reader,
		writer: writer,
	}, nil
}

func (s *Store) Close() error {
	return s.writer.Close()
}

func (s *Store) SpanReader() spanstore.Reader {
	return s.reader
}

func (s *Store) SpanWriter() spanstore.Writer {
	return s.writer
}

func (s *Store) DependencyReader() dependencystore.Reader {
	return s.reader
}
