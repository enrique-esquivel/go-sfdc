package credentials

import (
	"errors"
	"io"
	"net/url"
	"strings"
)

// RefreshTokenCredentials allows a client to
// obtain an access token
type RefreshTokenCredentials struct {
	URL          string
	RefreshToken string
	ClientID     string
	ClientSecret string
}

type refreshTokenProvider struct {
	creds RefreshTokenCredentials
}

func (provider *refreshTokenProvider) Retrieve() (io.Reader, error) {
	form := url.Values{}
	form.Add("grant_type", "refresh_token")
	form.Add("format", "json")
	form.Add("refresh_token", provider.creds.RefreshToken)
	form.Add("client_id", provider.creds.ClientID)
	form.Add("client_secret", provider.creds.ClientSecret)

	return strings.NewReader(form.Encode()), nil
}

func (provider *refreshTokenProvider) URL() string {
	return provider.creds.URL
}

// NewRefreshTokenCredentials allows you to
// initiate credentials using a refresh token from a previous login
func NewRefreshTokenCredentials(creds RefreshTokenCredentials) (*Credentials, error) {
	if err := validateRefreshTokenCredentials(creds); err != nil {
		return nil, err
	}
	return &Credentials{
		provider: &refreshTokenProvider{
			creds: creds,
		},
	}, nil
}

func validateRefreshTokenCredentials(cred RefreshTokenCredentials) error {
	if cred.URL == "" {
		return errors.New("credentials: password credential's URL can not be empty")
	}
	if cred.RefreshToken == "" {
		return errors.New("credentials: refresh token credential's refreshToken can not be empty")
	}
	if cred.ClientID == "" {
		return errors.New("credentials: password credential's client ID can not be empty")
	}
	if cred.ClientSecret == "" {
		return errors.New("credentials: password credential's client secret can not be empty")
	}
	return nil
}
