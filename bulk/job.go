package bulk

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/enrique-esquivel/go-sfdc"
	"github.com/enrique-esquivel/go-sfdc/session"
)

// JobType is the bulk job type.
type JobType string

const (
	// BigObjects is the big objects job.
	BigObjects JobType = "BigObjectIngest"
	// Classic is the bulk job 1.0.
	Classic JobType = "Classic"
	// V2Ingest is the bulk job 2.0.
	V2Ingest JobType = "V2Ingest"
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

// Operation is the processing operation for the job.
type Operation string

const (
	// Insert is the object operation for inserting records.
	Insert Operation = "insert"
	// Delete is the object operation for deleting records.
	Delete Operation = "delete"
	// Hard Delete is the object operation for hard deleting records.
	HardDelete Operation = "hardDelete"
	// Update is the object operation for updating records.
	Update Operation = "update"
	// Upsert is the object operation for upserting records.
	Upsert Operation = "upsert"
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

// UnprocessedRecord is the unprocessed records from the job.
type UnprocessedRecord struct {
	Fields map[string]string
}

// JobRecord is the record for the job.  Includes the Salesforce ID along with the fields.
type JobRecord struct {
	ID string
	UnprocessedRecord
}

// SuccessfulRecord indicates for the record was created and the data that was uploaded.
type SuccessfulRecord struct {
	Created bool
	JobRecord
}

// FailedRecord indicates why the record failed and the data of the record.
type FailedRecord struct {
	Error string
	JobRecord
}

// Options are the options for the job.
//
// ColumnDelimiter is the delimiter used for the CSV job.  This field is optional.
//
// ContentType is the content type for the job.  This field is optional.
//
// ExternalIDFieldName is the external ID field in the object being updated.  Only needed for
// upsert operations.  This field is required for upsert operations.
//
// LineEnding is the line ending used for the CSV job data.  This field is optional.
//
// Object is the object type for the data bneing processed. This field is required.
//
// Operation is the processing operation for the job. This field is required.
type Options struct {
	ColumnDelimiter     ColumnDelimiter `json:"columnDelimiter"`
	ContentType         ContentType     `json:"contentType"`
	ExternalIDFieldName string          `json:"externalIdFieldName"`
	LineEnding          LineEnding      `json:"lineEnding"`
	Object              string          `json:"object"`
	Operation           Operation       `json:"operation"`
}

// WriteResponse is the response to job APIs.
type WriteResponse struct {
	APIVersion          float32         `json:"apiVersion"`
	ColumnDelimiter     ColumnDelimiter `json:"columnDelimiter"`
	ConcurrencyMode     string          `json:"concurrencyMode"`
	ContentType         string          `json:"contentType"`
	ContentURL          string          `json:"contentUrl"`
	CreatedByID         string          `json:"createdById"`
	CreatedDate         string          `json:"createdDate"`
	ExternalIDFieldName string          `json:"externalIdFieldName"`
	ID                  string          `json:"id"`
	JobType             JobType         `json:"jobType"`
	LineEnding          LineEnding      `json:"lineEnding"`
	Object              string          `json:"object"`
	Operation           Operation       `json:"operation"`
	State               State           `json:"state"`
	SystemModstamp      string          `json:"systemModstamp"`
}

// Info is the response to the job information API.
type Info struct {
	WriteResponse
	ApexProcessingTime      int    `json:"apexProcessingTime"`
	APIActiveProcessingTime int    `json:"apiActiveProcessingTime"`
	NumberRecordsFailed     int    `json:"numberRecordsFailed"`
	NumberRecordsProcessed  int    `json:"numberRecordsProcessed"`
	Retries                 int    `json:"retries"`
	TotalProcessingTime     int    `json:"totalProcessingTime"`
	ErrorMessage            string `json:"errorMessage"`
}

// Job is the bulk job.
type Job struct {
	session       session.ServiceFormatter
	WriteResponse WriteResponse
}

func (j *Job) create(options Options) error {
	err := j.formatOptions(&options)
	if err != nil {
		return err
	}
	j.WriteResponse, err = j.createCallout(options)
	if err != nil {
		return err
	}

	return nil
}

func (j *Job) formatOptions(options *Options) error {
	if options.Operation == "" {
		return errors.New("bulk job: operation is required")
	}
	if options.Operation == Upsert {
		if options.ExternalIDFieldName == "" {
			return errors.New("bulk job: external id field name is required for upsert operation")
		}
	}
	if options.Object == "" {
		return errors.New("bulk job: object is required")
	}
	if options.LineEnding == "" {
		options.LineEnding = Linefeed
	}
	if options.ContentType == "" {
		options.ContentType = CSV
	}
	if options.ColumnDelimiter == "" {
		options.ColumnDelimiter = Comma
	}
	return nil
}

func (j *Job) createCallout(options Options) (WriteResponse, error) {
	url := j.session.ServiceURL() + bulk2Endpoint
	body, err := json.Marshal(options)
	if err != nil {
		return WriteResponse{}, err
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return WriteResponse{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.response(request)
}

func (j *Job) response(request *http.Request) (WriteResponse, error) {
	response, err := j.session.Client().Do(request)
	if err != nil {
		return WriteResponse{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return WriteResponse{}, sfdc.HandleError(response)
	}

	var value WriteResponse
	err = decoder.Decode(&value)
	if err != nil {
		return WriteResponse{}, err
	}
	return value, nil
}

// Info returns the current job information.
func (j *Job) Info() (Info, error) {
	return j.fetchInfo(j.WriteResponse.ID)
}

func (j *Job) fetchInfo(id string) (Info, error) {
	url := j.session.ServiceURL() + bulk2Endpoint + "/" + id
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return Info{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.infoResponse(request)
}

func (j *Job) infoResponse(request *http.Request) (Info, error) {
	response, err := j.session.Client().Do(request)
	if err != nil {
		return Info{}, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err := sfdc.HandleError(response)
		return Info{}, err
	}

	decoder := json.NewDecoder(response.Body)
	var value Info
	err = decoder.Decode(&value)
	if err != nil {
		return Info{}, err
	}
	return value, nil
}

func (j *Job) setState(state State) (WriteResponse, error) {
	url := j.session.ServiceURL() + bulk2Endpoint + "/" + j.WriteResponse.ID
	jobState := struct {
		State string `json:"state"`
	}{
		State: string(state),
	}
	body, err := json.Marshal(jobState)
	if err != nil {
		return WriteResponse{}, err
	}
	request, err := http.NewRequest(http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return WriteResponse{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.response(request)
}

// Close will close the current job.
func (j *Job) Close() (WriteResponse, error) {
	return j.setState(UpdateComplete)
}

// Abort will abort the current job.
func (j *Job) Abort() (WriteResponse, error) {
	return j.setState(Aborted)
}

// Delete will delete the current job.
func (j *Job) Delete() error {
	url := j.session.ServiceURL() + bulk2Endpoint + "/" + j.WriteResponse.ID
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

// Upload will upload data to processing.
func (j *Job) Upload(body io.Reader) error {
	url := j.session.ServiceURL() + bulk2Endpoint + "/" + j.WriteResponse.ID + "/batches"
	request, err := http.NewRequest(http.MethodPut, url, body)
	if err != nil {
		return err
	}
	request.Header.Add("Content-Type", "text/csv")
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		return sfdc.HandleError(response)
	}
	return nil
}

func (j *Job) getSuccessfulResults() (*http.Response, error) {
	url := j.session.ServiceURL() + bulk2Endpoint + "/" + j.WriteResponse.ID + "/successfulResults/"
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Accept", "text/csv")
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		return nil, sfdc.HandleError(response)
	}

	return response, nil
}

// ReadSuccessfulResults read job results from local file
func (j *Job) ReadSuccessfulResults(filename string) ([]SuccessfulRecord, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return j.ParseSuccessfulResults(f)
}

// ParseSuccessfulResults parse results of operation
func (j *Job) ParseSuccessfulResults(stream io.Reader) ([]SuccessfulRecord, error) {
	reader := csv.NewReader(stream)
	reader.Comma = j.delimiter()

	var records []SuccessfulRecord
	fields, err := reader.Read()
	if err != nil {
		return nil, err
	}
	for {
		values, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var record SuccessfulRecord
		created, err := strconv.ParseBool(values[j.headerPosition(sfCreated, fields)])
		if err != nil {
			return nil, err
		}
		record.Created = created
		record.ID = values[j.headerPosition(sfID, fields)]
		record.Fields = j.record(fields[2:], values[2:])
		records = append(records, record)
	}

	return records, nil
}

// SuccessfulRecords returns the successful records for the job.
func (j *Job) SuccessfulRecords() ([]SuccessfulRecord, error) {
	response, err := j.getSuccessfulResults()
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()
	return j.ParseSuccessfulResults(response.Body)
}

// ExportSuccessfulResults export failed results to file.
func (j *Job) ExportSuccessfulResults(filename string) error {
	response, err := j.getSuccessfulResults()
	if err != nil {
		return err
	}

	defer response.Body.Close()

	out, err := os.Create(filename)
	if err != nil {
		return err
	}

	defer out.Close()

	_, err = io.Copy(out, response.Body)
	return err
}

func (j *Job) getFailedResults() (*http.Response, error) {
	url := j.session.ServiceURL() + bulk2Endpoint + "/" + j.WriteResponse.ID + "/failedResults/"
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Accept", "text/csv")
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		return nil, sfdc.HandleError(response)
	}

	return response, nil
}

// ExportFailedResults export failed results to file.
func (j *Job) ExportFailedResults(filename string) error {
	response, err := j.getFailedResults()
	if err != nil {
		return err
	}

	defer response.Body.Close()

	out, err := os.Create(filename)
	if err != nil {
		return err
	}

	defer out.Close()

	_, err = io.Copy(out, response.Body)
	return err
}

// ReadFailedResults read job results from local file
func (j *Job) ReadFailedResults(filename string) ([]FailedRecord, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return j.ParseFailedResults(f)
}

// ParseFailedResults parse response from failedresults
func (j *Job) ParseFailedResults(stream io.Reader) ([]FailedRecord, error) {
	reader := csv.NewReader(stream)
	reader.Comma = j.delimiter()

	var records []FailedRecord
	fields, err := reader.Read()
	if err != nil {
		return nil, err
	}
	for {
		values, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var record FailedRecord
		record.Error = values[j.headerPosition(sfError, fields)]
		record.ID = values[j.headerPosition(sfID, fields)]
		record.Fields = j.record(fields[2:], values[2:])
		records = append(records, record)
	}

	return records, nil
}

// FailedRecords returns the failed records for the job.
func (j *Job) FailedRecords() ([]FailedRecord, error) {
	response, err := j.getFailedResults()
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	return j.ParseFailedResults(response.Body)
}

// UnprocessedRecords returns the unprocessed records for the job.
func (j *Job) UnprocessedRecords() ([]UnprocessedRecord, error) {
	url := j.session.ServiceURL() + bulk2Endpoint + "/" + j.WriteResponse.ID + "/unprocessedrecords/"
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Accept", "text/csv")
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, sfdc.HandleError(response)
	}

	reader := csv.NewReader(response.Body)
	reader.Comma = j.delimiter()

	var records []UnprocessedRecord
	fields, err := reader.Read()
	if err != nil {
		return nil, err
	}
	for {
		values, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var record UnprocessedRecord
		record.Fields = j.record(fields, values)
		records = append(records, record)
	}

	return records, nil
}

func (j *Job) headerPosition(column string, header []string) int {
	for idx, col := range header {
		if col == column {
			return idx
		}
	}
	return -1
}

func (j *Job) fields(header []string, offset int) []string {
	fields := make([]string, len(header)-offset)
	copy(fields[:], header[offset:])
	return fields
}

func (j *Job) record(fields, values []string) map[string]string {
	record := make(map[string]string)
	for idx, field := range fields {
		record[field] = values[idx]
	}
	return record
}

func (j *Job) delimiter() rune {
	switch ColumnDelimiter(j.WriteResponse.ColumnDelimiter) {
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
