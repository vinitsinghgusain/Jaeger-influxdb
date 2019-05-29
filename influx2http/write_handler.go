package influx2http

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
)

const (
	errInvalidPrecision = "invalid precision; valid precision units are ns, us, ms, and s"
	writePath           = "/api/v2/write"
)

// WriteService sends data over HTTP to influxdb via line protocol.
type WriteService struct {
	Addr               string
	Token              string
	Precision          string
	InsecureSkipVerify bool
}

var _ influxdb.WriteService = (*WriteService)(nil)

func (s *WriteService) Write(ctx context.Context, orgID, bucketID influxdb.ID, r io.Reader) error {
	precision := s.Precision
	if precision == "" {
		precision = "ns"
	}

	if !models.ValidPrecision(precision) {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   "http/Write",
			Msg:  errInvalidPrecision,
		}
	}

	u, err := NewURL(s.Addr, writePath)
	if err != nil {
		return err
	}

	r, err = compressWithGzip(r)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), r)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Content-Encoding", "gzip")
	SetToken(s.Token, req)

	org, err := orgID.Encode()
	if err != nil {
		return err
	}

	bucket, err := bucketID.Encode()
	if err != nil {
		return err
	}

	params := req.URL.Query()
	params.Set("org", string(org))
	params.Set("bucket", string(bucket))
	params.Set("precision", string(precision))
	req.URL.RawQuery = params.Encode()

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckError(resp)
}

func compressWithGzip(data io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)
	var err error

	go func() {
		_, err = io.Copy(gw, data)
		gw.Close()
		pw.Close()
	}()

	return pr, err
}
