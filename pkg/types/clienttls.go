package types

import "errors"

// Validate checks that the certificate and key files are set.
func (c *ClientTLSConfig) Validate() error {
	if c == nil {
		return errors.New("client TLS config cannot be nil")
	}
	if c.CertFile == "" || c.KeyFile == "" {
		return errors.New("client TLS needs a certificate file and a key file")
	}
	return nil
}

// Clone returns a copy of the configuration, nil for nil.
func (c *ClientTLSConfig) Clone() *ClientTLSConfig {
	if c == nil {
		return nil
	}
	cp := *c
	return &cp
}
