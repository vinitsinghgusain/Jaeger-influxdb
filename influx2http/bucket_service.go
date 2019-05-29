package influx2http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/influxdata/influxdb"
)

const (
	bucketPath = "/api/v2/buckets"
)

// BucketService connects to Influx via HTTP using tokens to manage buckets
type BucketService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
	// OpPrefix is an additional property for error
	// find bucket service, when finds nothing.
	OpPrefix string
}

// FindBucket returns the first bucket that matches filter.
func (s *BucketService) FindBucket(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
	bs, n, err := s.FindBuckets(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n == 0 && filter.Name != nil {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindBucket,
			Msg:  fmt.Sprintf("bucket %q not found", *filter.Name),
		}
	} else if n == 0 {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindBucket,
			Msg:  "bucket not found",
		}
	}

	return bs[0], nil
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
// Additional options provide pagination & sorting.
func (s *BucketService) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	u, err := NewURL(s.Addr, bucketPath)
	if err != nil {
		return nil, 0, err
	}

	query := u.Query()
	if filter.OrganizationID != nil {
		query.Add("orgID", filter.OrganizationID.String())
	}
	if filter.Org != nil {
		query.Add("org", *filter.Org)
	}
	if filter.ID != nil {
		query.Add("id", filter.ID.String())
	}
	if filter.Name != nil {
		query.Add("name", *filter.Name)
	}

	if len(opt) > 0 {
		for k, vs := range opt[0].QueryParams() {
			for _, v := range vs {
				query.Add(k, v)
			}
		}
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	req.URL.RawQuery = query.Encode()
	SetToken(s.Token, req)

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var bs bucketsResponse
	if err := json.NewDecoder(resp.Body).Decode(&bs); err != nil {
		return nil, 0, err
	}

	buckets := make([]*influxdb.Bucket, 0, len(bs.Buckets))
	for _, b := range bs.Buckets {
		pb, err := b.bucket.toInfluxDB()
		if err != nil {
			return nil, 0, err
		}

		buckets = append(buckets, pb)
	}

	return buckets, len(buckets), nil
}

// bucket is used for serialization/deserialization with duration string syntax.
type bucket struct {
	ID                  influxdb.ID     `json:"id,omitempty"`
	OrgID               influxdb.ID     `json:"orgID,omitempty"`
	Description         string          `json:"description,omitempty"`
	Name                string          `json:"name"`
	RetentionPolicyName string          `json:"rp,omitempty"` // This to support v1 sources
	RetentionRules      []retentionRule `json:"retentionRules"`
}

// retentionRule is the retention rule action for a bucket.
type retentionRule struct {
	Type         string `json:"type"`
	EverySeconds int64  `json:"everySeconds"`
}

func (b *bucket) toInfluxDB() (*influxdb.Bucket, error) {
	if b == nil {
		return nil, nil
	}

	var d time.Duration // zero value implies infinite retention policy

	// Only support a single retention period for the moment
	if len(b.RetentionRules) > 0 {
		d = time.Duration(b.RetentionRules[0].EverySeconds) * time.Second
		if d < time.Second {
			return nil, &influxdb.Error{
				Code: influxdb.EUnprocessableEntity,
				Msg:  "expiration seconds must be greater than or equal to one second",
			}
		}
	}

	return &influxdb.Bucket{
		ID:                  b.ID,
		OrgID:               b.OrgID,
		Description:         b.Description,
		Name:                b.Name,
		RetentionPolicyName: b.RetentionPolicyName,
		RetentionPeriod:     d,
	}, nil
}

type bucketsResponse struct {
	Links   *influxdb.PagingLinks `json:"links"`
	Buckets []*bucketResponse     `json:"buckets"`
}

type bucketResponse struct {
	bucket
	Links  map[string]string `json:"links"`
	Labels []influxdb.Label  `json:"labels"`
}
