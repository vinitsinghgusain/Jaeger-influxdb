package influx2http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/query"
)

const fluxPath = "/api/v2/query"

// FluxService connects to Influx via HTTP using tokens to run queries.
type FluxService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// Query runs a flux query against a influx server and sends the results to the io.Writer.
// Will use the token from the context over the token within the service struct.
func (s *FluxService) Query(ctx context.Context, w io.Writer, r *query.ProxyRequest) (flux.Statistics, error) {
	u, err := NewURL(s.Addr, fluxPath)
	if err != nil {
		return flux.Statistics{}, err
	}

    u.Query().Add("orgID", r.Request.OrganizationID.String())

	qreq, err := QueryRequestFromProxyRequest(r)
	if err != nil {
		return flux.Statistics{}, err
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qreq); err != nil {
		return flux.Statistics{}, err
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return flux.Statistics{}, err
	}

	SetToken(s.Token, hreq)

	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/csv")
	hreq = hreq.WithContext(ctx)

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return flux.Statistics{}, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return flux.Statistics{}, err
	}

	if _, err := io.Copy(w, resp.Body); err != nil {
		return flux.Statistics{}, err
	}
	return flux.Statistics{}, nil
}

// FluxQueryService implements query.QueryService by making HTTP requests to the /api/v2/query API endpoint.
type FluxQueryService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// Query runs a flux query against a influx server and decodes the result
func (s *FluxQueryService) Query(ctx context.Context, r *query.Request) (flux.ResultIterator, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(s.Addr, fluxPath)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	params := url.Values{}
	params.Set(OrgID, r.OrganizationID.String())
	u.RawQuery = params.Encode()

	preq := &query.ProxyRequest{
		Request: *r,
		Dialect: csv.DefaultDialect(),
	}
	qreq, err := QueryRequestFromProxyRequest(preq)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qreq); err != nil {
		return nil, tracing.LogError(span, err)
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	SetToken(s.Token, hreq)

	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/csv")
	hreq = hreq.WithContext(ctx)
	tracing.InjectToHTTPRequest(span, hreq)

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	// Can't defer resp.Body.Close here because the CSV decoder depends on reading from resp.Body after this function returns.

	if err := CheckError(resp); err != nil {
		return nil, tracing.LogError(span, err)
	}

	decoder := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	itr, err := decoder.Decode(resp.Body)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	return itr, nil
}

// QueryRequestFromProxyRequest converts a query.ProxyRequest into a QueryRequest.
// The ProxyRequest must contain supported compilers and dialects otherwise an error occurs.
func QueryRequestFromProxyRequest(req *query.ProxyRequest) (*QueryRequest, error) {
	qr := new(QueryRequest)
	switch c := req.Request.Compiler.(type) {
	case lang.FluxCompiler:
		qr.Type = "flux"
		qr.Query = c.Query
	case repl.Compiler:
		qr.Type = "flux"
		qr.Spec = c.Spec
	case lang.ASTCompiler:
		qr.Type = "flux"
		qr.AST = c.AST
	default:
		return nil, fmt.Errorf("unsupported compiler %T", c)
	}
	switch d := req.Dialect.(type) {
	case *csv.Dialect:
		var header = !d.ResultEncoderConfig.NoHeader
		qr.Dialect.Header = &header
		qr.Dialect.Delimiter = string(d.ResultEncoderConfig.Delimiter)
		qr.Dialect.CommentPrefix = "#"
		qr.Dialect.DateTimeFormat = "RFC3339"
		qr.Dialect.Annotations = d.ResultEncoderConfig.Annotations
	default:
		return nil, fmt.Errorf("unsupported dialect %T", d)
	}

	return qr, nil
}
