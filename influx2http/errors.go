package influx2http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/influxdb"
)

// CheckError reads the http.Response and returns an error if one exists.
// It will automatically recognize the errors returned by Influx services
// and decode the error into an internal error type. If the error cannot
// be determined in that way, it will create a generic error message.
//
// If there is no error, then this returns nil.
func CheckError(resp *http.Response) (err error) {
	switch resp.StatusCode / 100 {
	case 4, 5:
		// We will attempt to parse this error outside of this block.
	case 2:
		return nil
	default:
		// TODO(jsternberg): Figure out what to do here?
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("unexpected status code: %d %s", resp.StatusCode, resp.Status),
		}
	}
	pe := new(influxdb.Error)
	parseErr := json.NewDecoder(resp.Body).Decode(pe)
	if parseErr != nil {
		return parseErr
	}
	return pe
}
