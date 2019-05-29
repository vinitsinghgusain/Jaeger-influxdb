package influx2http

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/influxdata/influxdb"
)

const (
	organizationPath = "/api/v2/orgs"
)

// OrganizationService connects to Influx via HTTP using tokens to manage organizations.
type OrganizationService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
	// OpPrefix is for not found errors.
	OpPrefix string
}

// FindOrganization gets a single organization matching the filter using HTTP.
func (s *OrganizationService) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	if filter.ID == nil && filter.Name == nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "no filter parameters provided",
		}
	}
	os, n, err := s.FindOrganizations(ctx, filter)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Op:  s.OpPrefix + influxdb.OpFindOrganization,
		}
	}

	if n == 0 {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindOrganization,
			Msg:  "organization not found",
		}
	}

	return os[0], nil
}

// FindOrganizations returns all organizations that match the filter via HTTP.
func (s *OrganizationService) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	url, err := NewURL(s.Addr, organizationPath)
	if err != nil {
		return nil, 0, err
	}
	qp := url.Query()

	if filter.Name != nil {
		qp.Add(OrgName, *filter.Name)
	}
	if filter.ID != nil {
		qp.Add(OrgID, filter.ID.String())
	}
	url.RawQuery = qp.Encode()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	SetToken(s.Token, req)
	hc := NewClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var os orgsResponse
	if err := json.NewDecoder(resp.Body).Decode(&os); err != nil {
		return nil, 0, err
	}

	orgs := os.Toinfluxdb()
	return orgs, len(orgs), nil
}

type orgsResponse struct {
	Links         map[string]string `json:"links"`
	Organizations []*orgResponse    `json:"orgs"`
}

func (o orgsResponse) Toinfluxdb() []*influxdb.Organization {
	orgs := make([]*influxdb.Organization, len(o.Organizations))
	for i := range o.Organizations {
		orgs[i] = &o.Organizations[i].Organization
	}
	return orgs
}

type orgResponse struct {
	Links map[string]string `json:"links"`
	influxdb.Organization
}
