package nats

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/nats-io/nats-server/v2/server"
)

// applyClusterAuth sets the route credentials on opts: the server requires
// them from every route (cluster authorization), and every route URL without
// credentials of its own sends them. nats-server sends only the credentials of
// a route's URL on an explicit route, so without the second step a server
// would demand credentials its own routes never present. Empty username and
// password leave opts unchanged.
func applyClusterAuth(opts *server.Options, username, password string) error {
	if username == "" && password == "" {
		return nil
	}
	if opts.Cluster.Port == 0 {
		return errors.New("cluster credentials are set but clustering is not enabled: " +
			"use WithNATSClustering or a cluster{} block in the NATS config file")
	}
	opts.Cluster.Username = username
	opts.Cluster.Password = password
	for i, route := range opts.Routes {
		if route == nil || route.User != nil {
			continue
		}
		withAuth := *route
		withAuth.User = url.UserPassword(username, password)
		opts.Routes[i] = &withAuth
	}
	return nil
}

// validateClusteredJetStream checks the requirements nats-server places on
// clustered JetStream before the server is created, so that a missing setting
// is reported in the framework's terms. nats-server requires a server name, a
// cluster name and routes (or a solicited leafnode) whenever JetStream is
// enabled and a cluster port is set.
func validateClusteredJetStream(opts *server.Options) error {
	if !opts.JetStream || opts.Cluster.Port == 0 {
		return nil
	}
	if opts.ServerName == "" {
		return errors.New("clustered JetStream requires a server name that is unique within the cluster " +
			"and stable across restarts: set it with WithNATSServerName (for example the StatefulSet pod name) " +
			"or server_name in the NATS config file")
	}
	if err := ValidateServerName(opts.ServerName); err != nil {
		return fmt.Errorf("invalid server name: %w", err)
	}
	if opts.Cluster.Name == "" {
		return errors.New("clustered JetStream requires a cluster name: set it with WithNATSClustering " +
			"or cluster.name in the NATS config file")
	}
	// nats-server stops with a fatal log otherwise, which through the
	// embedded server only shows as a readiness timeout. Every server may
	// list all of them, its own included: a route to itself is ignored.
	if len(opts.Routes) == 0 && len(opts.LeafNode.Remotes) == 0 {
		return errors.New("clustered JetStream requires at least one route: list the cluster's servers " +
			"(this one included) in WithNATSClustering or cluster.routes in the NATS config file")
	}
	return nil
}

// ValidateRouteURL checks a cluster route URL: it must parse and have a scheme
// and a host, and nothing a route never has (a path, query, fragment or
// opaque part). The last rule matters for credentials: a password containing
// "/", "?" or "#" that is not percent-encoded ends the authority early, so the
// password would be parsed as a host, port or path and could end up in logs.
// Errors name the route by index and never quote it.
func ValidateRouteURL(index int, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid cluster route at index %d: not a valid URL (percent-encode special characters in credentials)", index)
	}
	if u.Scheme == "" || u.Host == "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("invalid cluster route at index %d: must be of the form nats://[user:password@]host:port "+
			"(percent-encode special characters in credentials)", index)
	}
	return nil
}

// redactURLs returns the URLs reduced to scheme, user name and host, for
// logging. An entry that is not a valid route URL is replaced entirely, since
// it may still hold credentials in an unexpected place.
func redactURLs(urls []string) []string {
	out := make([]string, len(urls))
	for i, raw := range urls {
		u, err := url.Parse(raw)
		if err != nil || ValidateRouteURL(i, raw) != nil {
			out[i] = "<invalid>"
			continue
		}
		safe := url.URL{Scheme: u.Scheme, Host: u.Host}
		if u.User != nil {
			safe.User = url.User(u.User.Username())
		}
		out[i] = safe.String()
	}
	return out
}
