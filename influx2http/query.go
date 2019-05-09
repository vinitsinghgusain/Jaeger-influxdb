package influx2http

import (
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/query"
)

// QueryRequest is a flux query request.
type QueryRequest struct {
	Extern  *ast.File    `json:"extern,omitempty"`
	Spec    *flux.Spec   `json:"spec,omitempty"`
	AST     *ast.Package `json:"ast,omitempty"`
	Query   string       `json:"query"`
	Type    string       `json:"type"`
	Dialect QueryDialect `json:"dialect"`

	Org *influxdb.Organization `json:"-"`
}

// QueryDialect is the formatting options for the query response.
type QueryDialect struct {
	Header         *bool    `json:"header"`
	Delimiter      string   `json:"delimiter"`
	CommentPrefix  string   `json:"commentPrefix"`
	DateTimeFormat string   `json:"dateTimeFormat"`
	Annotations    []string `json:"annotations"`
}

// WithDefaults adds default values to the request.
func (r QueryRequest) WithDefaults() QueryRequest {
	if r.Type == "" {
		r.Type = "flux"
	}
	if r.Dialect.Delimiter == "" {
		r.Dialect.Delimiter = ","
	}
	if r.Dialect.DateTimeFormat == "" {
		r.Dialect.DateTimeFormat = "RFC3339"
	}
	if r.Dialect.Header == nil {
		header := true
		r.Dialect.Header = &header
	}
	return r
}

// Validate checks the query request and returns an error if the request is invalid.
func (r QueryRequest) Validate() error {
	// TODO(jsternberg): Remove this, but we are going to not mention
	// the spec in the error if it is being used.
	if r.Query == "" && r.Spec == nil && r.AST == nil {
		return errors.New(`request body requires either query or AST`)
	}

	if r.Spec != nil && r.Extern != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "request body cannot specify both a spec and external declarations",
		}
	}

	if r.Type != "flux" {
		return fmt.Errorf(`unknown query type: %s`, r.Type)
	}

	if len(r.Dialect.CommentPrefix) > 1 {
		return fmt.Errorf("invalid dialect comment prefix: must be length 0 or 1")
	}

	if len(r.Dialect.Delimiter) != 1 {
		return fmt.Errorf("invalid dialect delimeter: must be length 1")
	}

	rune, size := utf8.DecodeRuneInString(r.Dialect.Delimiter)
	if rune == utf8.RuneError && size == 1 {
		return fmt.Errorf("invalid dialect delimeter character")
	}

	for _, a := range r.Dialect.Annotations {
		switch a {
		case "group", "datatype", "default":
		default:
			return fmt.Errorf(`unknown dialect annotation type: %s`, a)
		}
	}

	switch r.Dialect.DateTimeFormat {
	case "RFC3339", "RFC3339Nano":
	default:
		return fmt.Errorf(`unknown dialect date time format: %s`, r.Dialect.DateTimeFormat)
	}

	return nil
}

// ProxyRequest returns a request to proxy from the flux.
func (r QueryRequest) ProxyRequest() (*query.ProxyRequest, error) {
	return r.proxyRequest(time.Now)
}

func (r QueryRequest) proxyRequest(now func() time.Time) (*query.ProxyRequest, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	// Query is preferred over AST
	var compiler flux.Compiler
	if r.Query != "" {
		pkg, err := flux.Parse(r.Query)
		if err != nil {
			return nil, err
		}
		c := lang.ASTCompiler{
			AST: pkg,
			Now: now(),
		}
		if r.Extern != nil {
			c.PrependFile(r.Extern)
		}
		compiler = c
	} else if r.AST != nil {
		c := lang.ASTCompiler{
			AST: r.AST,
			Now: now(),
		}
		if r.Extern != nil {
			c.PrependFile(r.Extern)
		}
		compiler = c
	} else if r.Spec != nil {
		compiler = repl.Compiler{
			Spec: r.Spec,
		}
	}

	delimiter, _ := utf8.DecodeRuneInString(r.Dialect.Delimiter)

	noHeader := false
	if r.Dialect.Header != nil {
		noHeader = !*r.Dialect.Header
	}

	// TODO(nathanielc): Use commentPrefix and dateTimeFormat
	// once they are supported.
	return &query.ProxyRequest{
		Request: query.Request{
			OrganizationID: r.Org.ID,
			Compiler:       compiler,
		},
		Dialect: &csv.Dialect{
			ResultEncoderConfig: csv.ResultEncoderConfig{
				NoHeader:    noHeader,
				Delimiter:   delimiter,
				Annotations: r.Dialect.Annotations,
			},
		},
	}, nil
}
