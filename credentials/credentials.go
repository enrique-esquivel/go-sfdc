package credentials

import (
	"errors"
	"io"
)

// Credentials is the structure that contains all of the
// information for creating a session.
type Credentials struct {
	provider Provider
}

// Provider is the interface that is able to provide the
// session creator with all of the valid information.
//
// Retrieve will return the reader for the HTTP request body.
//
// URL is the URL base for the session endpoint.
type Provider interface {
	Retrieve() (io.Reader, error)
	URL() string
}

// Retrieve will return the reader for the HTTP request body.
func (creds *Credentials) Retrieve() (io.Reader, error) {
	return creds.provider.Retrieve()
}

// URL is the URL base for the session endpoint.
func (creds *Credentials) URL() string {
	return creds.provider.URL()
}

// NewCredentials will create a credential with the custom provider.
func NewCredentials(provider Provider) (*Credentials, error) {
	if provider == nil {
		return nil, errors.New("credentials: the provider can not be nil")
	}
	return &Credentials{
		provider: provider,
	}, nil
}
