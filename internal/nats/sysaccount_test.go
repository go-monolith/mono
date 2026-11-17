package nats

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
)

type customAuth struct{}

func (customAuth) Check(server.ClientAuthentication) bool { return true }

func TestApplySystemAccount(t *testing.T) {
	t.Run("empty leaves the options unchanged", func(t *testing.T) {
		opts := &server.Options{}
		if err := applySystemAccount(opts, "", "", false); err != nil {
			t.Fatal(err)
		}
		if opts.SystemAccount != "" || len(opts.Accounts) != 0 || len(opts.Users) != 0 {
			t.Errorf("options changed: %+v", opts)
		}
	})

	t.Run("adds a $SYS user and keeps clients without credentials allowed", func(t *testing.T) {
		opts := &server.Options{}
		if err := applySystemAccount(opts, "admin", "secret", false); err != nil {
			t.Fatal(err)
		}
		if opts.SystemAccount != "$SYS" || len(opts.Accounts) != 1 || opts.Accounts[0].Name != "$SYS" {
			t.Fatalf("system account = %q, accounts = %v", opts.SystemAccount, opts.Accounts)
		}
		if len(opts.Users) != 1 || opts.Users[0].Username != "admin" || opts.Users[0].Password != "secret" || opts.Users[0].Account != opts.Accounts[0] {
			t.Fatalf("users = %+v", opts.Users)
		}
		if opts.NoAuthUser != "" || opts.Authorization != "" {
			t.Error("must not configure client authorization")
		}
	})

	reject := []struct {
		name      string
		opts      server.Options
		authBlock bool
		user      string
		pass      string
		reason    string
	}{
		{"missing password", server.Options{}, false, "admin", "", "cannot be empty"},
		{"missing username", server.Options{}, false, "", "secret", "cannot be empty"},
		{"operator", server.Options{TrustedKeys: []string{"O..."}}, false, "admin", "secret", "operator"},
		{"no system account", server.Options{NoSystemAccount: true}, false, "admin", "secret", "no_sys_acc"},
		{"config file accounts", server.Options{Accounts: []*server.Account{server.NewAccount("APP")}}, false, "admin", "secret", "accounts or system_account"},
		{"config file system account", server.Options{SystemAccount: "SYS"}, false, "admin", "secret", "accounts or system_account"},
		{"client users", server.Options{Users: []*server.User{{Username: "app"}}}, false, "admin", "secret", "client authorization"},
		{"client token", server.Options{Authorization: "token"}, false, "admin", "secret", "client authorization"},
		{"single user", server.Options{Username: "app", Password: "pw"}, false, "admin", "secret", "client authorization"},
		{"nkeys", server.Options{Nkeys: []*server.NkeyUser{{Nkey: "U..."}}}, false, "admin", "secret", "client authorization"},
		{"no auth user", server.Options{NoAuthUser: "app"}, false, "admin", "secret", "client authorization"},
		{"trusted operators", server.Options{TrustedOperators: []*jwt.OperatorClaims{{}}}, false, "admin", "secret", "operator"},
		{"auth callout", server.Options{AuthCallout: &server.AuthCallout{}}, false, "admin", "secret", "client authorization"},
		{"custom client authentication", server.Options{CustomClientAuthentication: customAuth{}}, false, "admin", "secret", "client authorization"},
		{"empty authorization block", server.Options{}, true, "admin", "secret", "client authorization"},
	}
	for _, tt := range reject {
		t.Run("rejects "+tt.name, func(t *testing.T) {
			opts := tt.opts
			err := applySystemAccount(&opts, tt.user, tt.pass, tt.authBlock)
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.reason)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Errorf("error leaks the password: %v", err)
			}
		})
	}
}

func TestWithSystemAccount(t *testing.T) {
	cfg := &NATSConfig{}
	if err := WithSystemAccount("", "pw")(cfg); err == nil {
		t.Error("empty username accepted")
	}
	if err := WithSystemAccount("admin", "")(cfg); err == nil {
		t.Error("empty password accepted")
	}
	if err := WithSystemAccount("admin", "pw")(cfg); err != nil || cfg.SystemAccountUsername != "admin" || cfg.SystemAccountPassword != "pw" {
		t.Errorf("err = %v, cfg = %+v", err, cfg)
	}
}

func TestConfigHasAuthorizationBlock(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		conf string
		want bool
	}{
		{"port: 4222\n", false},
		{"authorization {}\n", true},
		{"authorization { timeout: 5 }\n", true},
		{"Authorization { users: [] }\n", true},
		{"accounts { \"$SYS\": { users: [ { user: admin, password: pw } ] } }\nsystem_account: \"$SYS\"\n", false},
	} {
		p := filepath.Join(dir, "n.conf")
		if err := os.WriteFile(p, []byte(tc.conf), 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := configHasAuthorizationBlock(p)
		if err != nil || got != tc.want {
			t.Errorf("%q: got %v, %v; want %v", tc.conf, got, err, tc.want)
		}
	}
	if _, err := configHasAuthorizationBlock(filepath.Join(dir, "missing.conf")); err == nil {
		t.Error("a missing file must be an error")
	}
}
