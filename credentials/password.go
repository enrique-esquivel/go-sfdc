package credentials

import (
	"errors"
	"io"
	"net/url"
	"strings"
)

type grantType string

const (
	passwordGrantType grantType = "password"
)

// PasswordCredentials is a structure for the OAuth credentials
// that are needed to authenticate with a Salesforce org.
//
// URL is the login URL used, examples would be https://test.salesforce.com or https://login.salesforce.com
//
// Username is the Salesforce user name for logging into the org.
//
// Password is the Salesforce password for the user.
//
// ClientID is the client ID from the connected application.
//
// ClientSecret is the client secret from the connected application.
type PasswordCredentials struct {
	URL          string
	Username     string
	Password     string
	ClientID     string
	ClientSecret string
}

type passwordProvider struct {
	creds PasswordCredentials
}

func (provider *passwordProvider) Retrieve() (io.Reader, error) {
	form := url.Values{}
	form.Add("grant_type", string(passwordGrantType))
	form.Add("username", provider.creds.Username)
	form.Add("password", provider.creds.Password)
	form.Add("client_id", provider.creds.ClientID)
	form.Add("client_secret", provider.creds.ClientSecret)

	return strings.NewReader(form.Encode()), nil
}

func (provider *passwordProvider) URL() string {
	return provider.creds.URL
}

// NewPasswordCredentials will create a credential with the password credentials.
func NewPasswordCredentials(creds PasswordCredentials) (*Credentials, error) {
	if err := validatePasswordCredentials(creds); err != nil {
		return nil, err
	}
	return &Credentials{
		provider: &passwordProvider{
			creds: creds,
		},
	}, nil
}

func validatePasswordCredentials(cred PasswordCredentials) error {
	if cred.URL == "" {
		return errors.New("credentials: password credential's URL can not be empty")
	}
	if cred.Username == "" {
		return errors.New("credentials: password credential's username can not be empty")
	}
	if cred.Password == "" {
		return errors.New("credentials: password credential's password can not be empty")
	}
	if cred.ClientID == "" {
		return errors.New("credentials: password credential's client ID can not be empty")
	}
	if cred.ClientSecret == "" {
		return errors.New("credentials: password credential's client secret can not be empty")
	}
	return nil
}
