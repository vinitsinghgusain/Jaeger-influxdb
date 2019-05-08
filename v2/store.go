package v2

import (
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

type V2Store struct {
}

func (s *V2Store) DependencyReader() dependencystore.Reader {
	panic("non implemented")
}

func (s *V2Store) SpanReader() spanstore.Reader {
	panic("non implemented")
}

func (s *V2Store) SpanWriter() spanstore.Writer {
	panic("non implemented")
}
