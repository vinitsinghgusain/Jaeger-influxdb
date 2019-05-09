package dbmodel

import (
	"time"

	"github.com/jaegertracing/jaeger/model"
)

/*
If SpanID were an InfluxDB tag, then each span would be it's own series, which is wasteful.
InfluxDB appreciates attention to cardinality, so SpanID is an InfluxDB field.

When querying traces, we group results by (1) TraceID and (2) SpanID.
The fields in a Span share SpanID *and* timestamp, so Span identity is TraceID:timestamp as much as it is TraceID:SpanID.
Therefore, our Flux queries efficiently group by TraceID and timestamp.

There is a *tiny* risk that two spans in a single trace share one timestamp.
We reduce this risk with a little hack:

Jaeger timestamps are microsecond precision, and InfluxDB timestamps are nanosecond precision.
Therefore, the 3 least significant decimal digits in InfluxDB timestamps are unused.
To translate from a Jaeger SpanID (uint64) and timestamp (µs) to an InfluxDB timestamp (ns),
we pad the µs timestamp with a poor-man's hash of the SpanID.
To translate from a InfluxDB timestamp back to a Jaeger timestamp, simply truncate the least three significant digits.
*/

const primeDenominator = 997

// mergeTimeAndSpanID converts Jaeger [timestamp as µs, SpanID as uint64] to InfluxDB [timestamp as ns].
func mergeTimeAndSpanID(t time.Time, spanID model.SpanID) time.Time {
	nanoseconds := t.UnixNano()
	nanoseconds -= nanoseconds % 1000 // % 1000 should be zero, but just in case
	spanSuffix := int64(spanID % primeDenominator)
	return time.Unix(0, nanoseconds+spanSuffix)
}

// removeSpanIDFromTime truncates
func removeSpanIDFromTime(t int64) time.Time {
	return time.Unix(0, t-t%1000)
}
