package common

import (
	"sync"
	"time"
)

// WriterMetaCache throttles
type WriterMetaCache struct {
	sync.RWMutex
	m      map[string]time.Time // [service name]-[operation name] -> [expiration time]
	maxAge time.Duration
}

func NewWriterMetaCache(maxAge time.Duration) *WriterMetaCache {
	return &WriterMetaCache{
		m:      make(map[string]time.Time),
		maxAge: maxAge,
	}
}

func (c *WriterMetaCache) ShouldWrite(serviceName, operationName string, startTime time.Time) (shouldWrite bool) {
	k := serviceName + "-" + operationName

	c.RLock()
	v, found := c.m[k]
	c.RUnlock()

	if !found || startTime.After(v) { // Key doesn't exist, or is expired.
		c.Lock()
		if v, found = c.m[k]; !found || startTime.After(v) { // Double check with write lock
			c.m[k] = startTime.Add(c.maxAge)
			shouldWrite = true
		}
		c.Unlock()
	}

	return
}
