package soql

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/enrique-esquivel/go-sfdc"
	"github.com/enrique-esquivel/go-sfdc/session"
	"github.com/pkg/errors"
)

// Resource is the structure for the Salesforce
// SOQL API resource.
type Resource struct {
	session session.ServiceFormatter
}

// NewResource forms the Salesforce SOQL resource. The
// session formatter is required to form the proper URLs and authorization
// header.
func NewResource(session session.ServiceFormatter) (*Resource, error) {
	if session == nil {
		return nil, errors.New("soql: session can not be nil")
	}

	err := session.Refresh()
	if err != nil {
		return nil, errors.Wrap(err, "session refresh")
	}

	return &Resource{
		session: session,
	}, nil
}

// Query will call out to the Salesforce org for a SOQL.  The results will
// be the result of the query.  The all parameter is for querying all records,
// which include deleted records that are in the recycle bin.
func (r *Resource) Query(querier QueryFormatter, all bool) (*QueryResult, error) {
	if querier == nil {
		return nil, errors.New("soql resource query: querier can not be nil")
	}

	request, err := r.queryRequest(querier, all)
	if err != nil {
		return nil, err
	}

	response, err := r.queryResponse(request)
	if err != nil {
		return nil, err
	}

	result, err := newQueryResult(response, r)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (r *Resource) next(recordURL string) (*QueryResult, error) {
	queryURL := r.session.InstanceURL() + recordURL
	request, err := http.NewRequest(http.MethodGet, queryURL, nil)

	if err != nil {
		return nil, err
	}

	request.Header.Add("Accept", "application/json")
	r.session.AuthorizationHeader(request)

	response, err := r.queryResponse(request)
	if err != nil {
		return nil, err
	}

	result, err := newQueryResult(response, r)
	if err != nil {
		return nil, err
	}

	return result, nil
}
func (r *Resource) queryRequest(querier QueryFormatter, all bool) (*http.Request, error) {
	query, err := querier.Format()
	if err != nil {
		return nil, err
	}

	endpoint := "/query"
	if all {
		endpoint += "All"
	}

	queryURL := r.session.ServiceURL() + endpoint + "/"

	form := url.Values{}
	form.Add("q", query)
	queryURL += "?" + form.Encode()

	request, err := http.NewRequest(http.MethodGet, queryURL, nil)

	if err != nil {
		return nil, err
	}

	request.Header.Add("Accept", "application/json")
	r.session.AuthorizationHeader(request)
	return request, nil

}
func (r *Resource) queryResponse(request *http.Request) (queryResponse, error) {
	response, err := r.session.Client().Do(request)

	if err != nil {
		return queryResponse{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return queryResponse{}, sfdc.HandleError(response)
	}

	var resp queryResponse
	err = decoder.Decode(&resp)
	if err != nil {
		return queryResponse{}, err
	}

	return resp, nil
}
