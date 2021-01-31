package bulkquery

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/crochik/go-sfdc"
	"github.com/crochik/go-sfdc/session"
)

// QueryJobType is the bulk job type.
type QueryJobType string

const (
	// V2Query is the big objects job.
	V2Query QueryJobType = "V2Query"
)

// ColumnDelimiter is the column delimiter used for CSV job data.
type ColumnDelimiter string

const (
	// Backquote is the (`) character.
	Backquote ColumnDelimiter = "BACKQUOTE"
	// Caret is the (^) character.
	Caret ColumnDelimiter = "CARET"
	// Comma is the (,) character.
	Comma ColumnDelimiter = "COMMA"
	// Pipe is the (|) character.
	Pipe ColumnDelimiter = "PIPE"
	// SemiColon is the (;) character.
	SemiColon ColumnDelimiter = "SEMICOLON"
	// Tab is the (\t) character.
	Tab ColumnDelimiter = "TAB"
)

// ContentType is the format of the data being processed.
type ContentType string

// CSV is the supported content data type.
const CSV ContentType = "CSV"

// LineEnding is the line ending used for the CSV job data.
type LineEnding string

const (
	// Linefeed is the (\n) character.
	Linefeed LineEnding = "LF"
	// CarriageReturnLinefeed is the (\r\n) character.
	CarriageReturnLinefeed LineEnding = "CRLF"
)

// QueryOperation is the processing operation for the job.
type QueryOperation string

const (
	// Query Returns data that has not been deleted or archived
	Query QueryOperation = "query"
	// QueryAll Returns records that have been deleted because of a merge or delete, and returns information about archived Task and Event records
	QueryAll QueryOperation = "queryAll"
)

// State is the current state of processing for the job.
type State string

const (
	// Open the job has been created and job data can be uploaded tothe job.
	Open State = "Open"
	// UpdateComplete all data for the job has been uploaded and the job is ready to be queued and processed.
	UpdateComplete State = "UploadComplete"
	// Aborted the job has been aborted.
	Aborted State = "Aborted"
	// JobComplete the job was processed by Salesforce.
	JobComplete State = "JobComplete"
	// Failed some records in the job failed.
	Failed State = "Failed"
)

const (
	// sfID is the column name for the Salesforce Object ID in Job CSV responses
	sfID = "sf__Id"

	// sfError is the column name for the error in Failed record responses
	sfError = "sf__Error"

	// sfError is the column name for the created flag in Successful record responses
	sfCreated = "sf__Created"
)

// QueryOptions are the options for the job.
//
// ColumnDelimiter is the delimiter used for the CSV job.  This field is optional.
//
// ContentType is the content type for the job.  This field is optional.
//
// LineEnding is the line ending used for the CSV job data.  This field is optional.
//
// QueryOptions is the processing operation for the job. This field is required.
type QueryOptions struct {
	ColumnDelimiter ColumnDelimiter `json:"columnDelimiter"`
	ContentType     ContentType     `json:"contentType"`
	LineEnding      LineEnding      `json:"lineEnding"`
	Query           string          `json:"query"`
	Operation       QueryOperation  `json:"operation"`
}

// QueryResponse is the response to job APIs.
type QueryResponse struct {
	APIVersion      float32         `json:"apiVersion"`
	ColumnDelimiter ColumnDelimiter `json:"columnDelimiter"`
	ConcurrencyMode string          `json:"concurrencyMode"`
	ContentType     string          `json:"contentType"`
	CreatedByID     string          `json:"createdById"`
	CreatedDate     string          `json:"createdDate"`
	ID              string          `json:"id"`
	JobType         QueryJobType    `json:"jobType"`
	LineEnding      LineEnding      `json:"lineEnding"`
	Object          string          `json:"object"`
	Operation       QueryOperation  `json:"operation"`
	State           State           `json:"state"`
	SystemModstamp  string          `json:"systemModstamp"`
}

// QueryInfo is the response to the job information API.
type QueryInfo struct {
	QueryResponse
	NumberRecordsProcessed int `json:"numberRecordsProcessed"`
	Retries                int `json:"retries"`
	TotalProcessingTime    int `json:"totalProcessingTime"`
}

// QueryJob is the bulk job.
type QueryJob struct {
	session       session.ServiceFormatter
	QueryResponse QueryResponse
}

func (j *QueryJob) create(options QueryOptions) error {
	err := j.formatOptions(&options)
	if err != nil {
		return err
	}
	j.QueryResponse, err = j.createCallout(options)
	if err != nil {
		return err
	}

	return nil
}

func (j *QueryJob) formatOptions(options *QueryOptions) error {
	if options.Query == "" {
		return errors.New("bulk job: query is required")
	}

	// defaults
	if options.LineEnding == "" {
		options.LineEnding = Linefeed
	}

	if options.ContentType == "" {
		options.ContentType = CSV
	}

	if options.ColumnDelimiter == "" {
		options.ColumnDelimiter = Comma
	}

	if options.Operation == "" {
		options.Operation = Query
	}

	return nil
}

func (j *QueryJob) createCallout(options QueryOptions) (QueryResponse, error) {
	url := j.session.ServiceURL() + bulk2Endpoint
	body, err := json.Marshal(options)
	if err != nil {
		return QueryResponse{}, err
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return QueryResponse{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.response(request)
}

func (j *QueryJob) response(request *http.Request) (QueryResponse, error) {
	response, err := j.session.Client().Do(request)
	if err != nil {
		return QueryResponse{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return QueryResponse{}, sfdc.HandleError(response)
	}

	var value QueryResponse
	err = decoder.Decode(&value)
	if err != nil {
		return QueryResponse{}, err
	}

	j.QueryResponse = value

	return value, nil
}

// ExportResults exports the job results to a local file
// returns the next locator (if more results are available)
func (j *QueryJob) ExportResults(filepath string, maxRecords int, locator string) (string, error) {
	url := j.session.ServiceURL() + bulk2Endpoint + "/" + j.QueryResponse.ID + "/results"
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	q := request.URL.Query()
	if locator != "" {
		q.Add("locator", locator)
	}
	if maxRecords > 0 {
		q.Add("maxRecords", strconv.Itoa(maxRecords))
	}

	request.URL.RawQuery = q.Encode()

	request.Header.Add("Accept", "text/csv")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err := sfdc.HandleError(response)
		return "", err
	}

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return "", err
	}

	defer out.Close()

	// Writer the body to file
	_, err = io.Copy(out, response.Body)
	if err != nil {
		return "", err
	}

	newLocator := response.Header.Get("Sforce-Locator")
	return newLocator, nil
}

// Info returns the current job information.
func (j *QueryJob) Info() (QueryInfo, error) {
	return j.fetchInfo(j.QueryResponse.ID)
}

func (j *QueryJob) fetchInfo(id string) (QueryInfo, error) {
	url := j.session.ServiceURL() + bulk2Endpoint + "/" + id
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return QueryInfo{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.infoResponse(request)
}

func (j *QueryJob) infoResponse(request *http.Request) (QueryInfo, error) {
	response, err := j.session.Client().Do(request)
	if err != nil {
		return QueryInfo{}, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err := sfdc.HandleError(response)
		return QueryInfo{}, err
	}

	decoder := json.NewDecoder(response.Body)
	var value QueryInfo
	err = decoder.Decode(&value)
	if err != nil {
		return QueryInfo{}, err
	}

	j.QueryResponse = value.QueryResponse

	return value, nil
}

func (j *QueryJob) setState(state State) (QueryResponse, error) {
	url := j.session.ServiceURL() + bulk2Endpoint + "/" + j.QueryResponse.ID
	jobState := struct {
		State string `json:"state"`
	}{
		State: string(state),
	}
	body, err := json.Marshal(jobState)
	if err != nil {
		return QueryResponse{}, err
	}
	request, err := http.NewRequest(http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return QueryResponse{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.response(request)
}

// Abort will abort the current job.
func (j *QueryJob) Abort() (QueryResponse, error) {
	return j.setState(Aborted)
}

// Delete will delete the current job.
func (j *QueryJob) Delete() error {
	url := j.session.ServiceURL() + bulk2Endpoint + "/" + j.QueryResponse.ID
	request, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusNoContent {
		return errors.New("job error: unable to delete job")
	}
	return nil
}

func (j *QueryJob) headerPosition(column string, header []string) int {
	for idx, col := range header {
		if col == column {
			return idx
		}
	}
	return -1
}

func (j *QueryJob) fields(header []string, offset int) []string {
	fields := make([]string, len(header)-offset)
	copy(fields[:], header[offset:])
	return fields
}

func (j *QueryJob) record(fields, values []string) map[string]string {
	record := make(map[string]string)
	for idx, field := range fields {
		record[field] = values[idx]
	}
	return record
}

func (j *QueryJob) delimiter() rune {
	switch ColumnDelimiter(j.QueryResponse.ColumnDelimiter) {
	case Tab:
		return '\t'
	case SemiColon:
		return ';'
	case Pipe:
		return '|'
	case Caret:
		return '^'
	case Backquote:
		return '`'
	default:
		return ','
	}
}
