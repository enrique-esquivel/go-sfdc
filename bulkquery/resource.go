package bulkquery

import (
	"github.com/crochik/go-sfdc/session"
	"github.com/pkg/errors"
)

const bulk2Endpoint = "/jobs/query"

// Resource is the structure that can be used to create bulk 2.0 jobs.
type Resource struct {
	session session.ServiceFormatter
}

// NewResource creates a new bulk 2.0 REST resource.  If the session is nil
// an error will be returned.
func NewResource(session session.ServiceFormatter) (*Resource, error) {
	if session == nil {
		return nil, errors.New("bulk: session can not be nil")
	}

	err := session.Refresh()
	if err != nil {
		return nil, errors.Wrap(err, "session refresh")
	}

	return &Resource{
		session: session,
	}, nil
}

func (r *Resource) String() string {
	return "Bulk(Query)"
}

// CreateJob will create a new bulk 2.0 job from the options that where passed.
// The Job that is returned can be used to upload object data to the Salesforce org.
func (r *Resource) CreateJob(options QueryOptions) (*QueryJob, error) {
	job := &QueryJob{
		session: r.session,
	}
	if err := job.create(options); err != nil {
		return nil, err
	}

	return job, nil
}

// GetJob will retrieve an existing bulk 2.0 job using the provided ID.
func (r *Resource) GetJob(id string) (*QueryJob, error) {
	job := &QueryJob{
		session: r.session,
	}
	info, err := job.fetchInfo(id)
	if err != nil {
		return nil, err
	}
	job.QueryResponse = info.QueryResponse

	return job, nil
}

// // AllJobs will retrieve all of the bulk 2.0 jobs.
// func (r *Resource) AllJobs(parameters Parameters) (*Jobs, error) {
// 	jobs, err := newJobs(r.session, parameters)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return jobs, nil
// }
