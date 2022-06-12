package bulkv1

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"

	"github.com/enrique-esquivel/go-sfdc"
	"github.com/enrique-esquivel/go-sfdc/session"
)

const bulkEndpoint string = "job"

// ContentType is the format of the data being processed.
type ContentType string

// CSV is the supported content data type.
const (
	CSV      ContentType = "CSV"
	JSON     ContentType = "JSON"
	XML      ContentType = "XML"
	ZIP_CSV  ContentType = "ZIP_CSV"
	ZIP_JSON ContentType = "ZIP_JSON"
	ZIP_XML  ContentType = "ZIP_XML"
)

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
	// Open the job has been created and job data can be uploaded tothe job.
	Closed State = "Closed"
	// Aborted the job has been aborted.
	Aborted State = "Aborted"
	// Failed some records in the job failed.
	Failed State = "Failed"
)

// State is the current state of batch processing.
type BatchState string

const (
	// Processing of the batch hasn’t started yet. If the job associated with this batch is aborted,
	// the batch isn’t processed and its state is set to NotProcessed.
	Queue BatchState = "Open"
	// The batch is being processed. If the job associated with the batch is aborted,
	// the batch is still processed to completion. You must close the job associated with the batch so that the batch can finish processing.
	InProgress BatchState = "InProgress"
	// The batch has been processed completely, and the result resource is available. The result resource indicates if some records failed.
	// A batch can be completed even if some or all the records failed. If a subset of records failed, the successful records aren’t rolled back.
	Completed State = "Completed"
	// The batch failed to process the full request due to an unexpected error, such as the request is compressed with an unsupported format,
	// or an internal server error.
	BatchFailed State = "Failed"
	// The batch won’t be processed. This state is assigned when a job is aborted while the batch is queued. For bulk queries,
	// if the job has PK chunking enabled, this state is assigned to the original batch that contains the query when the subsequent batches are created
	NotProcessed State = "NotProcessed"
)

type Header string

const (
	PKChunkingHeader    Header = "Sforce-Enable-PKChunking"
	LineEndingHeader    Header = "Sforce-Line-Ending"
	ContetTypeHeader    Header = "Content-Type"
	SalesforceOpsHeader Header = "Sforce-Call-Options"
)

type ConcurrencyModeEnum string

const (
	Parallel ConcurrencyModeEnum = "Parallel"
	Serial   ConcurrencyModeEnum = "Serial"
)

//JobInfo
// A job contains one or more batches of data for you to submit to Salesforce for processing.
// When a job is created, Salesforce sets the job state to Open.

type JobInfo struct {
	APIVersion              float32             `json:"apiVersion"`
	ApexProcessingTime      int                 `json:"apexProcessingTime"`
	ApiActiveProcessingTime int                 `json:"apiActiveProcessingTime"`
	AssignmentRuleId        string              `json:"assignmentRuleId"`
	ConcurrencyMode         ConcurrencyModeEnum `json:"concurrencyMode"`
	ContentType             ContentType         `json:"contentType"`
	CreatedByID             string              `json:"createdById"`
	CreatedDate             string              `json:"createdDate"`
	ExternalIDFieldName     string              `json:"externalIdFieldName"`
	ID                      string              `json:"id"`
	NumberBatchesCompleted  int                 `json:"numberBatchesCompleted"`
	NumberBatchesQueued     int                 `json:"numberBatchesQueued"`
	NumberBatchesFailed     int                 `json:"numberBatchesFailed"`
	NumberBatchesInProgress int                 `json:"numberBatchesInProgress"`
	NumberBatchesTotal      int                 `json:"numberBatchesTotal"`
	NumberRecordsFailed     int                 `json:"numberRecordsFailed"`
	NumberRecordsProcessed  int                 `json:"numberRecordsProcessed"`
	NumberRetries           int                 `json:"numberRetries"`
	Object                  string              `json:"object"`
	Operation               Operation           `json:"operation"`
	State                   State               `json:"state"`
	SystemModstamp          string              `json:"systemModstamp"`
	TotalProcessingTime     int                 `json:"totalProcessingTime"`
}

//BatchInfo
// A BatchInfo contains one batch of data for you to submit to Salesforce for processing.

type BatchInfo struct {
	ApexProcessingTime      int        `json:"apexProcessingTime"`
	ApiActiveProcessingTime int        `json:"apiActiveProcessingTime"`
	CreatedDate             string     `json:"createdDate"`
	ID                      string     `json:"id"`
	JobID                   string     `json:"jobId"`
	NumberRecordsFailed     int        `json:"numberRecordsFailed"`
	NumberRecordsProcessed  int        `json:"numberRecordsProcessed"`
	State                   BatchState `json:"state"`
	StateMessage            string     `json:"stateMessage"`
	SystemModstamp          string     `json:"systemModstamp"`
	TotalProcessingTime     int        `json:"totalProcessingTime"`
}

type HeaderOptions struct {
	LineEnding  LineEnding
	ContentType ContentType
	Client      string
	PKChunking  string
}

// Options
// Information that must travel through headers
type Options struct {
	ContentType         ContentType `json:"contentType"`
	ExternalIDFieldName string      `json:"externalIdFieldName"`
	Object              string      `json:"object"`
	Operation           Operation   `json:"operation"`
}

// Job is the bulk job.
type Job struct {
	session  session.AsyncServiceFormatter
	Response JobInfo
}

func (j *Job) create(options Options, header HeaderOptions) error {
	err := j.formatOptions(options, &header)
	if err != nil {
		return err
	}
	j.Response, err = j.createCallout(options, header)
	if err != nil {
		return err
	}

	return nil
}

func (j *Job) formatOptions(options Options, header *HeaderOptions) error {
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
	if header.LineEnding == "" {
		header.LineEnding = Linefeed
	}
	if header.ContentType == "" {
		header.ContentType = CSV
	}
	if header.PKChunking == "" {
		header.PKChunking = "TRUE"
	}
	return nil
}

func (j *Job) createCallout(options Options, header HeaderOptions) (JobInfo, error) {
	url := j.session.AsyncServiceURL() + bulkEndpoint
	body, err := json.Marshal(options)
	if err != nil {
		return JobInfo{}, err
	}

	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return JobInfo{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add(string(PKChunkingHeader), header.PKChunking)
	request.Header.Add(string(LineEndingHeader), string(header.LineEnding))
	request.Header.Add(string(ContetTypeHeader), string(header.ContentType))

	j.session.AuthorizationHeader(request)

	return j.response(request)
}

func (j *Job) response(request *http.Request) (JobInfo, error) {
	response, err := j.session.Client().Do(request)
	if err != nil {
		return JobInfo{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return JobInfo{}, sfdc.HandleError(response)
	}

	var value JobInfo
	err = decoder.Decode(&value)
	if err != nil {
		return JobInfo{}, err
	}
	return value, nil
}

func (j *Job) createBatch(body io.Reader) (BatchInfo, error) {
	url := j.session.AsyncServiceURL() + bulkEndpoint + "/" + j.Response.ID + "/batch"
	request, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return BatchInfo{}, err
	}
	request.Header.Add("Content-Type", "text/csv")
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return BatchInfo{}, err
	}
	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		return BatchInfo{}, sfdc.HandleError(response)
	}
	var value BatchInfo
	err = decoder.Decode(&value)
	if err != nil {
		return BatchInfo{}, err
	}
	return value, nil
}

// Info returns the current job information.
func (j *Job) BatchInfo(info BatchInfo) (BatchInfo, error) {
	return j.fetchBatchInfo(j.Response.ID, info.ID)
}

func (j *Job) infoResponse(request *http.Request) (BatchInfo, error) {
	response, err := j.session.Client().Do(request)
	if err != nil {
		return BatchInfo{}, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err := sfdc.HandleError(response)
		return BatchInfo{}, err
	}

	decoder := json.NewDecoder(response.Body)
	var value BatchInfo
	err = decoder.Decode(&value)
	if err != nil {
		return BatchInfo{}, err
	}
	return value, nil
}
func (j *Job) fetchBatchInfo(jobId, batchId string) (BatchInfo, error) {
	url := j.session.ServiceURL() + bulkEndpoint + "/" + jobId + "/batch/" + batchId
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return BatchInfo{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.infoResponse(request)
}

func (j *Job) setState(state State) (JobInfo, error) {
	url := j.session.ServiceURL() + bulkEndpoint + "/" + j.Response.ID
	jobState := struct {
		State string `json:"state"`
	}{
		State: string(state),
	}
	body, err := json.Marshal(jobState)
	if err != nil {
		return JobInfo{}, err
	}
	request, err := http.NewRequest(http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return JobInfo{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.response(request)
}

// Close will close the current job.
func (j *Job) Close() (JobInfo, error) {
	return j.setState(Closed)
}

// Abort will abort the current job.
func (j *Job) Abort() (JobInfo, error) {
	return j.setState(Aborted)
}

// Delete will delete the current job.
func (j *Job) Delete() error {
	url := j.session.ServiceURL() + bulkEndpoint + "/" + j.Response.ID
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

func (j *Job) getResults(batchInfo BatchInfo) (*http.Response, error) {
	url := j.session.ServiceURL() + bulkEndpoint + "/" + j.Response.ID + "/batch/" + batchInfo.ID + "/result"
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

// ExportSuccessfulResults export failed results to file.
func (j *Job) ExportResults(filename string, batchInfo BatchInfo) error {
	response, err := j.getResults(batchInfo)
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
