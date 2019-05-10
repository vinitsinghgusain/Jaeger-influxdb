package dbmodel

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/influxdata/jaeger-influxdb/common"
	"github.com/jaegertracing/jaeger/model"
)

func referencesFromString(s string) ([]model.SpanRef, error) {
	var spanReferences []model.SpanRef

	for _, reference := range strings.Split(s, ",") {
		if reference == "" {
			continue
		}
		parts := strings.SplitN(reference, ":", 2)
		if len(parts) < 2 {
			return nil, errors.New("invalid span reference")
		}
		spanID, err := model.SpanIDFromString(parts[0])
		if err != nil {
			return nil, err
		}
		var refType model.SpanRefType
		switch parts[1] {
		case common.ReferenceTypeChildOf:
			refType = model.SpanRefType_CHILD_OF
		case common.ReferenceTypeFollowsFrom:
			refType = model.SpanRefType_FOLLOWS_FROM
		default:
			return nil, errors.New("unrecognized span reference type")
		}
		spanRef := model.SpanRef{
			SpanID:  spanID,
			RefType: refType,
		}
		spanReferences = append(spanReferences, spanRef)
	}

	return spanReferences, nil
}

// keyValueAsStrings converts a model.KeyValue to two strings,
// for use as InfluxDB tag key and value.
func keyValueAsStrings(kv *model.KeyValue) (string, string, error) {
	var valueTypePrefix, valueAsString string

	switch kv.VType {
	case model.ValueType_STRING:
		valueTypePrefix = "s"
		valueAsString = kv.VStr
	case model.ValueType_BOOL:
		valueTypePrefix = "b"
		if kv.VBool {
			valueAsString = "t"
		} else {
			valueAsString = "f"
		}
	case model.ValueType_INT64:
		valueTypePrefix = "i"
		valueAsString = strconv.FormatInt(kv.VInt64, 10)
	case model.ValueType_FLOAT64:
		valueTypePrefix = "f"
		valueAsString = strconv.FormatFloat(kv.VFloat64, 'E', -1, 64)
	case model.ValueType_BINARY:
		valueTypePrefix = "B"
		valueAsString = base64.StdEncoding.EncodeToString(kv.VBinary)
	default:
		return "", "", errors.New("skipped unrecognized span kv value type")
	}

	return kv.Key, valueTypePrefix + ":" + valueAsString, nil
}

// keyValueAsStringAndInterface converts a model.KeyValue to a string and an interface,
// for use as InfluxDB field key and value.
func keyValueAsStringAndInterface(kv *model.KeyValue) (string, interface{}, error) {
	k := kv.Key
	var v interface{}

	switch kv.VType {
	case model.ValueType_STRING:
		v = "s" + strings.ReplaceAll(kv.VStr, "\n", "NEWLINE")
	case model.ValueType_BOOL:
		v = kv.VBool
	case model.ValueType_INT64:
		v = kv.VInt64
	case model.ValueType_FLOAT64:
		v = kv.VFloat64
	case model.ValueType_BINARY:
		v = kv.VBinary
		v = "B" + base64.StdEncoding.EncodeToString(kv.VBinary)
	default:
		return "", nil, fmt.Errorf("unrecognized kv value type '%s'", kv.VType.String())
	}

	return k, v, nil
}
