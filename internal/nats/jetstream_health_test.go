package nats

import (
	"testing"

	"github.com/nats-io/nats-server/v2/server"
)

func TestJetStreamHealthFromJsz(t *testing.T) {
	tests := []struct {
		name          string
		info          server.JSInfo
		available     bool
		clustered     bool
		leader        string
		size          int
		wantMessaging bool
	}{
		{"single server", server.JSInfo{}, true, false, "", 0, false},
		{"disabled", server.JSInfo{Disabled: true}, false, false, "", 0, true},
		{"cluster with leader", server.JSInfo{Meta: &server.MetaClusterInfo{Leader: "n-1", Size: 3}}, true, true, "n-1", 3, false},
		{"cluster without quorum", server.JSInfo{Meta: &server.MetaClusterInfo{Size: 2}}, false, true, "", 2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := jetStreamHealthFromJsz(&tt.info)
			if h.Available != tt.available || h.Clustered != tt.clustered || h.MetaLeader != tt.leader || h.ClusterSize != tt.size {
				t.Errorf("health = %+v", h)
			}
			if (h.Message != "") != tt.wantMessaging {
				t.Errorf("message = %q", h.Message)
			}
		})
	}
}

func TestNATSManager_JetStreamHealth(t *testing.T) {
	m := &natsManager{}
	if h := m.JetStreamHealth(); h != nil {
		t.Errorf("not running: %+v, want nil", h)
	}

	without, err := startClusterNode(t, 0)
	if err != nil {
		t.Fatal(err)
	}
	if h := without.JetStreamHealth(); h != nil {
		t.Errorf("without JetStream: %+v, want nil", h)
	}

	with, err := startClusterNode(t, 0, WithJetStream(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	h := with.JetStreamHealth()
	if h == nil || !h.Available || h.Clustered {
		t.Errorf("single JetStream server: %+v", h)
	}
}
