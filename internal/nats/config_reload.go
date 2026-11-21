package nats

import "github.com/nats-io/nats-server/v2/server"

// DetachConfigFile removes the config file path from opts before the
// embedded server is created with them, so that the server refuses a
// configuration reload.
//
// server.ProcessConfigFile records the path in the options, and the server
// keeps it: a reload request (`$SYS.REQ.SERVER.<id>.RELOAD`, sent by
// `nats server config reload` with system account credentials) makes it
// parse the file again and apply the result on its own. What the framework's
// options added to the parsed file exists only in the options, so a single
// reload switched off JetStream, client TLS or AutoTLS (plaintext clients were
// let in), route authorization and route TLS, and the system account user,
// while the framework logged nothing. Applying the options again on a reload
// would need a hook nats-server does not have; the configuration is fixed at
// NewMonoApplication instead, as it is everywhere else in the framework, and
// a reload is answered with nats-server's error "can only reload config when
// a file is provided using -c or --config". Signal-triggered reloads are
// already off (NoSigs).
//
// The path stays in the framework's own configuration and log; only
// nats-server's own start-up line naming the file is no longer written.
func DetachConfigFile(opts *server.Options) {
	opts.ConfigFile = ""
}
