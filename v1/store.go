package v1

import (
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

type V1Store struct {
}

func (s *V1Store) DependencyReader() dependencystore.Reader {
	panic("non implemented")
}

func (s *V1Store) SpanReader() spanstore.Reader {
	panic("non implemented")
}

func (s *V1Store) SpanWriter() spanstore.Writer {
	panic("non implemented")
}
