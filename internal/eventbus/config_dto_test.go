package eventbus

import (
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// TestToJetStreamStreamConfig tests stream config conversion
func TestToJetStreamStreamConfig(t *testing.T) {
	t.Run("valid config with minimal fields", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST_STREAM",
			Subjects: []string{"test.>"},
		}

		jsCfg, err := toJetStreamStreamConfig(cfg)
		if err != nil {
			t.Fatalf("toJetStreamStreamConfig failed: %v", err)
		}

		if jsCfg.Name != "TEST_STREAM" {
			t.Errorf("expected name TEST_STREAM, got %s", jsCfg.Name)
		}
		if len(jsCfg.Subjects) != 1 || jsCfg.Subjects[0] != "test.>" {
			t.Errorf("expected subjects [test.>], got %v", jsCfg.Subjects)
		}
	})

	t.Run("empty name returns error", func(t *testing.T) {
		cfg := types.StreamConfig{
			Subjects: []string{"test.>"},
		}

		_, err := toJetStreamStreamConfig(cfg)
		if err == nil {
			t.Fatal("expected error for empty name")
		}
		if err.Error() != "stream name is required" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("empty subjects returns error (non-mirror)", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name: "TEST",
		}

		_, err := toJetStreamStreamConfig(cfg)
		if err == nil {
			t.Fatal("expected error for empty subjects")
		}
		if err.Error() != "at least one subject is required (unless stream is a mirror)" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("mirror stream without subjects is valid", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name: "MIRROR_STREAM",
			Mirror: &types.StreamSource{
				Name: "SOURCE_STREAM",
			},
		}

		jsCfg, err := toJetStreamStreamConfig(cfg)
		if err != nil {
			t.Fatalf("toJetStreamStreamConfig failed for mirror: %v", err)
		}

		if jsCfg.Name != "MIRROR_STREAM" {
			t.Errorf("expected name MIRROR_STREAM, got %s", jsCfg.Name)
		}
	})

	t.Run("invalid retention policy", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:      "TEST",
			Subjects:  []string{"test.>"},
			Retention: 99, // Invalid
		}

		_, err := toJetStreamStreamConfig(cfg)
		if err == nil {
			t.Fatal("expected error for invalid retention policy")
		}
	})

	t.Run("valid retention policies", func(t *testing.T) {
		retentionPolicies := []types.RetentionPolicy{
			types.LimitsPolicy,
			types.InterestPolicy,
			types.WorkQueuePolicy,
		}

		for _, policy := range retentionPolicies {
			cfg := types.StreamConfig{
				Name:      "TEST",
				Subjects:  []string{"test.>"},
				Retention: policy,
			}

			jsCfg, err := toJetStreamStreamConfig(cfg)
			if err != nil {
				t.Errorf("toJetStreamStreamConfig failed for retention policy %d: %v", policy, err)
			}
			if jsCfg.Name == "" {
				t.Error("config conversion failed")
			}
		}
	})

	t.Run("invalid storage type", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
			Storage:  99, // Invalid
		}

		_, err := toJetStreamStreamConfig(cfg)
		if err == nil {
			t.Fatal("expected error for invalid storage type")
		}
	})

	t.Run("valid storage types", func(t *testing.T) {
		storageTypes := []types.StorageType{
			types.FileStorage,
			types.MemoryStorage,
		}

		for _, storage := range storageTypes {
			cfg := types.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"test.>"},
				Storage:  storage,
			}

			jsCfg, err := toJetStreamStreamConfig(cfg)
			if err != nil {
				t.Errorf("toJetStreamStreamConfig failed for storage type %d: %v", storage, err)
			}
			if jsCfg.Name == "" {
				t.Error("config conversion failed")
			}
		}
	})

	t.Run("invalid discard policy", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
			Discard:  99, // Invalid
		}

		_, err := toJetStreamStreamConfig(cfg)
		if err == nil {
			t.Fatal("expected error for invalid discard policy")
		}
	})

	t.Run("valid discard policies", func(t *testing.T) {
		discardPolicies := []types.DiscardPolicy{
			types.DiscardOld,
			types.DiscardNew,
		}

		for _, discard := range discardPolicies {
			cfg := types.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"test.>"},
				Discard:  discard,
			}

			jsCfg, err := toJetStreamStreamConfig(cfg)
			if err != nil {
				t.Errorf("toJetStreamStreamConfig failed for discard policy %d: %v", discard, err)
			}
			if jsCfg.Name == "" {
				t.Error("config conversion failed")
			}
		}
	})

	t.Run("invalid compression", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:        "TEST",
			Subjects:    []string{"test.>"},
			Compression: 99, // Invalid
		}

		_, err := toJetStreamStreamConfig(cfg)
		if err == nil {
			t.Fatal("expected error for invalid compression")
		}
	})

	t.Run("valid compression types", func(t *testing.T) {
		compressionTypes := []types.StoreCompression{
			types.NoCompression,
			types.S2Compression,
		}

		for _, compression := range compressionTypes {
			cfg := types.StreamConfig{
				Name:        "TEST",
				Subjects:    []string{"test.>"},
				Compression: compression,
			}

			jsCfg, err := toJetStreamStreamConfig(cfg)
			if err != nil {
				t.Errorf("toJetStreamStreamConfig failed for compression %d: %v", compression, err)
			}
			if jsCfg.Name == "" {
				t.Error("config conversion failed")
			}
		}
	})

	t.Run("config with placement", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
			Placement: &types.Placement{
				Cluster: "cluster1",
				Tags:    []string{"tag1", "tag2"},
			},
		}

		jsCfg, err := toJetStreamStreamConfig(cfg)
		if err != nil {
			t.Fatalf("toJetStreamStreamConfig failed: %v", err)
		}

		if jsCfg.Placement == nil {
			t.Fatal("placement is nil")
		}
		if jsCfg.Placement.Cluster != "cluster1" {
			t.Errorf("expected cluster cluster1, got %s", jsCfg.Placement.Cluster)
		}
	})

	t.Run("config with sources", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
			Sources: []*types.StreamSource{
				{Name: "source1"},
				{Name: "source2"},
			},
		}

		jsCfg, err := toJetStreamStreamConfig(cfg)
		if err != nil {
			t.Fatalf("toJetStreamStreamConfig failed: %v", err)
		}

		if len(jsCfg.Sources) != 2 {
			t.Errorf("expected 2 sources, got %d", len(jsCfg.Sources))
		}
	})

	t.Run("config with subject transform", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
			SubjectTransform: &types.SubjectTransformConfig{
				Source:      "test.>",
				Destination: "transformed.>",
			},
		}

		jsCfg, err := toJetStreamStreamConfig(cfg)
		if err != nil {
			t.Fatalf("toJetStreamStreamConfig failed: %v", err)
		}

		if jsCfg.SubjectTransform == nil {
			t.Fatal("subject transform is nil")
		}
		if jsCfg.SubjectTransform.Source != "test.>" {
			t.Errorf("expected source test.>, got %s", jsCfg.SubjectTransform.Source)
		}
	})

	t.Run("config with republish", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
			RePublish: &types.RePublish{
				Source:      "test.>",
				Destination: "republish.>",
				HeadersOnly: true,
			},
		}

		jsCfg, err := toJetStreamStreamConfig(cfg)
		if err != nil {
			t.Fatalf("toJetStreamStreamConfig failed: %v", err)
		}

		if jsCfg.RePublish == nil {
			t.Fatal("republish is nil")
		}
		if !jsCfg.RePublish.HeadersOnly {
			t.Error("expected HeadersOnly to be true")
		}
	})

	t.Run("config with consumer limits", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
			ConsumerLimits: types.StreamConsumerLimits{
				InactiveThreshold: 5 * time.Second,
				MaxAckPending:     100,
			},
		}

		jsCfg, err := toJetStreamStreamConfig(cfg)
		if err != nil {
			t.Fatalf("toJetStreamStreamConfig failed: %v", err)
		}

		if jsCfg.ConsumerLimits.InactiveThreshold != 5*time.Second {
			t.Errorf("expected InactiveThreshold 5s, got %v", jsCfg.ConsumerLimits.InactiveThreshold)
		}
		if jsCfg.ConsumerLimits.MaxAckPending != 100 {
			t.Errorf("expected MaxAckPending 100, got %d", jsCfg.ConsumerLimits.MaxAckPending)
		}
	})

	t.Run("config with all optional fields", func(t *testing.T) {
		cfg := types.StreamConfig{
			Name:                 "TEST",
			Subjects:             []string{"test.>"},
			Description:          "Test stream",
			MaxConsumers:         10,
			MaxMsgs:              1000,
			MaxBytes:             1024 * 1024,
			DiscardNewPerSubject: true,
			MaxAge:               24 * time.Hour,
			MaxMsgsPerSubject:    100,
			MaxMsgSize:           1024,
			Replicas:             3,
			NoAck:                true,
			Duplicates:           5 * time.Second,
			Sealed:               false,
			DenyDelete:           false,
			DenyPurge:            false,
			AllowRollup:          true,
			FirstSeq:             1,
			AllowDirect:          true,
			MirrorDirect:         false,
			Metadata:             map[string]string{"key": "value"},
		}

		jsCfg, err := toJetStreamStreamConfig(cfg)
		if err != nil {
			t.Fatalf("toJetStreamStreamConfig failed: %v", err)
		}

		if jsCfg.Description != "Test stream" {
			t.Errorf("expected description 'Test stream', got %s", jsCfg.Description)
		}
		if jsCfg.MaxConsumers != 10 {
			t.Errorf("expected MaxConsumers 10, got %d", jsCfg.MaxConsumers)
		}
		if jsCfg.Replicas != 3 {
			t.Errorf("expected Replicas 3, got %d", jsCfg.Replicas)
		}
	})
}

// TestToJetStreamStreamSource tests stream source conversion
func TestToJetStreamStreamSource(t *testing.T) {
	t.Run("nil source", func(t *testing.T) {
		result := toJetStreamStreamSource(nil)
		if result != nil {
			t.Error("expected nil for nil source")
		}
	})

	t.Run("minimal source", func(t *testing.T) {
		src := &types.StreamSource{
			Name: "source",
		}

		jsSrc := toJetStreamStreamSource(src)
		if jsSrc == nil {
			t.Fatal("result is nil")
		}
		if jsSrc.Name != "source" {
			t.Errorf("expected name 'source', got %s", jsSrc.Name)
		}
	})

	t.Run("source with all fields", func(t *testing.T) {
		src := &types.StreamSource{
			Name:          "source",
			OptStartSeq:   100,
			OptStartTime:  &time.Time{},
			FilterSubject: "filter.>",
			Domain:        "domain1",
			SubjectTransforms: []types.SubjectTransformConfig{
				{Source: "a.>", Destination: "b.>"},
			},
			External: &types.ExternalStream{
				APIPrefix:     "prefix",
				DeliverPrefix: "deliver",
			},
		}

		jsSrc := toJetStreamStreamSource(src)
		if jsSrc == nil {
			t.Fatal("result is nil")
		}
		if jsSrc.Name != "source" {
			t.Errorf("expected name 'source', got %s", jsSrc.Name)
		}
		if jsSrc.Domain != "domain1" {
			t.Errorf("expected domain 'domain1', got %s", jsSrc.Domain)
		}
		if len(jsSrc.SubjectTransforms) != 1 {
			t.Errorf("expected 1 subject transform, got %d", len(jsSrc.SubjectTransforms))
		}
		if jsSrc.External == nil {
			t.Fatal("external is nil")
		}
		if jsSrc.External.APIPrefix != "prefix" {
			t.Errorf("expected APIPrefix 'prefix', got %s", jsSrc.External.APIPrefix)
		}
	})
}

// TestToJetStreamConsumerConfig tests consumer config conversion
func TestToJetStreamConsumerConfig(t *testing.T) {
	t.Run("valid minimal config", func(t *testing.T) {
		cfg := types.ConsumerConfig{
			Name: "consumer",
		}

		jsCfg, err := toJetStreamConsumerConfig(cfg)
		if err != nil {
			t.Fatalf("toJetStreamConsumerConfig failed: %v", err)
		}

		if jsCfg.Name != "consumer" {
			t.Errorf("expected name 'consumer', got %s", jsCfg.Name)
		}
	})

	t.Run("invalid ack policy", func(t *testing.T) {
		cfg := types.ConsumerConfig{
			Name:      "consumer",
			AckPolicy: 99, // Invalid
		}

		_, err := toJetStreamConsumerConfig(cfg)
		if err == nil {
			t.Fatal("expected error for invalid ack policy")
		}
	})

	t.Run("valid ack policies", func(t *testing.T) {
		ackPolicies := []types.AckPolicy{
			types.AckExplicitPolicy,
			types.AckNonePolicy,
			types.AckAllPolicy,
		}

		for _, policy := range ackPolicies {
			cfg := types.ConsumerConfig{
				Name:      "consumer",
				AckPolicy: policy,
			}

			jsCfg, err := toJetStreamConsumerConfig(cfg)
			if err != nil {
				t.Errorf("toJetStreamConsumerConfig failed for ack policy %d: %v", policy, err)
			}
			if jsCfg.Name == "" {
				t.Error("config conversion failed")
			}
		}
	})

	t.Run("invalid deliver policy", func(t *testing.T) {
		cfg := types.ConsumerConfig{
			Name:          "consumer",
			DeliverPolicy: 99, // Invalid
		}

		_, err := toJetStreamConsumerConfig(cfg)
		if err == nil {
			t.Fatal("expected error for invalid deliver policy")
		}
	})

	t.Run("valid deliver policies", func(t *testing.T) {
		deliverPolicies := []types.DeliverPolicy{
			types.DeliverAllPolicy,
			types.DeliverLastPolicy,
			types.DeliverNewPolicy,
			types.DeliverByStartSequencePolicy,
			types.DeliverByStartTimePolicy,
			types.DeliverLastPerSubjectPolicy,
		}

		for _, policy := range deliverPolicies {
			cfg := types.ConsumerConfig{
				Name:          "consumer",
				DeliverPolicy: policy,
			}

			jsCfg, err := toJetStreamConsumerConfig(cfg)
			if err != nil {
				t.Errorf("toJetStreamConsumerConfig failed for deliver policy %d: %v", policy, err)
			}
			if jsCfg.Name == "" {
				t.Error("config conversion failed")
			}
		}
	})

	t.Run("invalid replay policy", func(t *testing.T) {
		cfg := types.ConsumerConfig{
			Name:         "consumer",
			ReplayPolicy: 99, // Invalid
		}

		_, err := toJetStreamConsumerConfig(cfg)
		if err == nil {
			t.Fatal("expected error for invalid replay policy")
		}
	})

	t.Run("valid replay policies", func(t *testing.T) {
		replayPolicies := []types.ReplayPolicy{
			types.ReplayInstantPolicy,
			types.ReplayOriginalPolicy,
		}

		for _, policy := range replayPolicies {
			cfg := types.ConsumerConfig{
				Name:         "consumer",
				ReplayPolicy: policy,
			}

			jsCfg, err := toJetStreamConsumerConfig(cfg)
			if err != nil {
				t.Errorf("toJetStreamConsumerConfig failed for replay policy %d: %v", policy, err)
			}
			if jsCfg.Name == "" {
				t.Error("config conversion failed")
			}
		}
	})

	t.Run("config with all optional fields", func(t *testing.T) {
		cfg := types.ConsumerConfig{
			Name:               "consumer",
			Description:        "Test consumer",
			OptStartSeq:        100,
			OptStartTime:       &time.Time{},
			AckWait:            30 * time.Second,
			MaxDeliver:         3,
			BackOff:            []time.Duration{1 * time.Second, 2 * time.Second},
			FilterSubject:      "filter.>",
			RateLimit:          1024,
			SampleFrequency:    "50%",
			MaxWaiting:         100,
			MaxAckPending:      200,
			HeadersOnly:        true,
			MaxRequestBatch:    10,
			MaxRequestExpires:  5 * time.Second,
			MaxRequestMaxBytes: 1024,
			InactiveThreshold:  1 * time.Minute,
			Replicas:           3,
			MemoryStorage:      true,
			FilterSubjects:     []string{"a.>", "b.>"},
			Metadata:           map[string]string{"key": "value"},
		}

		jsCfg, err := toJetStreamConsumerConfig(cfg)
		if err != nil {
			t.Fatalf("toJetStreamConsumerConfig failed: %v", err)
		}

		if jsCfg.Description != "Test consumer" {
			t.Errorf("expected description 'Test consumer', got %s", jsCfg.Description)
		}
		if jsCfg.MaxDeliver != 3 {
			t.Errorf("expected MaxDeliver 3, got %d", jsCfg.MaxDeliver)
		}
		if jsCfg.Replicas != 3 {
			t.Errorf("expected Replicas 3, got %d", jsCfg.Replicas)
		}
		if !jsCfg.MemoryStorage {
			t.Error("expected MemoryStorage to be true")
		}
		if len(jsCfg.FilterSubjects) != 2 {
			t.Errorf("expected 2 filter subjects, got %d", len(jsCfg.FilterSubjects))
		}
	})
}
