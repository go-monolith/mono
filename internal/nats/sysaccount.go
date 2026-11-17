package nats

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats-server/v2/conf"
	"github.com/nats-io/nats-server/v2/server"
)

// systemAccountName is nats-server's default system account. Every server of
// a cluster must use the same system account name; servers without this
// option use the default too, so a cluster can enable it one server at a time.
const systemAccountName = server.DEFAULT_SYSTEM_ACCOUNT

// applySystemAccount adds a user of the system account ($SYS) to opts, for
// operator tooling: nats server report, nats server raft peer-remove (removing
// a server from the JetStream meta group, needed to retire servers for good)
// and the other $SYS requests. Empty username and password leave opts
// unchanged.
//
// It relies on nats-server's "system account only" mode: when the only
// accounts are the global one and the system account, no authorization is
// configured and there is no operator, nats-server keeps accepting clients
// without credentials (in the global account, where the application's
// streams and consumers live) and only the system account user needs a
// password. So enabling it changes nothing for the application's own client
// or for other clients that connect without credentials, and existing
// JetStream data stays where it is. A client that sends credentials (a URL
// with user info, a token) was accepted regardless before; it is now checked
// and rejected unless they are the system account user's.
//
// The mode does not apply when a config file configures accounts, client
// authorization or an operator; then nats-server would require credentials
// from every client, including the framework's own, so the combination is
// rejected. Add the system account user to that config file instead.
//
// authBlock reports whether the config file has an authorization block: any
// such block, even one without credentials (authorization {}, a timeout
// only), turns the mode off, and it cannot be told from the parsed options.
func applySystemAccount(opts *server.Options, username, password string, authBlock bool) error {
	if username == "" && password == "" {
		return nil
	}
	if username == "" || password == "" {
		return errors.New("system account username and password cannot be empty")
	}
	switch {
	case len(opts.TrustedOperators) > 0 || len(opts.TrustedKeys) > 0:
		return errors.New("WithNATSSystemAccount cannot be combined with an operator (decentralized auth) in the NATS config file; " +
			"use the operator's system account instead")
	case opts.NoSystemAccount:
		return errors.New("WithNATSSystemAccount cannot be combined with no_sys_acc in the NATS config file")
	case len(opts.Accounts) > 0 || opts.SystemAccount != "":
		return errors.New("WithNATSSystemAccount cannot be combined with accounts or system_account in the NATS config file; " +
			"add the system account user to the config file instead")
	case authBlock || opts.Username != "" || opts.Password != "" || opts.Authorization != "" ||
		len(opts.Users) > 0 || len(opts.Nkeys) > 0 || opts.NoAuthUser != "" ||
		opts.AuthCallout != nil || opts.CustomClientAuthentication != nil:
		return errors.New("WithNATSSystemAccount cannot be combined with client authorization in the NATS config file: " +
			"every client, the framework's own included, would then need credentials; " +
			"add the system account user to the config file instead")
	}
	sys := server.NewAccount(systemAccountName)
	// Clients without credentials keep connecting through the no-auth user
	// nats-server adds; clients that send credentials are checked from now
	// on and rejected unless they match the system account user.
	opts.Accounts = append(opts.Accounts, sys)
	opts.SystemAccount = systemAccountName
	opts.Users = append(opts.Users, &server.User{Username: username, Password: password, Account: sys})
	return nil
}

// configHasAuthorizationBlock reports whether the NATS config file at path has
// a top-level authorization block (keys are case-insensitive, as nats-server
// reads them).
func configHasAuthorizationBlock(path string) (bool, error) {
	m, err := conf.ParseFile(path)
	if err != nil {
		return false, fmt.Errorf("failed to parse NATS config file %q: %w", path, err)
	}
	for k := range m {
		if strings.EqualFold(k, "authorization") {
			return true, nil
		}
	}
	return false, nil
}
