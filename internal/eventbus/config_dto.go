package eventbus

import (
	"fmt"

	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go/jetstream"
)

// toJetStreamStreamConfig converts types.StreamConfig to jetstream.StreamConfig.
// It validates retention policy, storage type, discard policy, compression, and required
// fields before conversion, returning an error if invalid values are provided.
//
// Returns an error if:
//   - cfg.Name is empty
//   - cfg.Subjects is empty (unless Mirror is set)
//   - cfg.Retention is not a valid RetentionPolicy enum value
//   - cfg.Storage is not a valid StorageType enum value
//   - cfg.Discard is not a valid DiscardPolicy enum value
//   - cfg.Compression is not a valid StoreCompression enum value
func toJetStreamStreamConfig(cfg types.StreamConfig) (jetstream.StreamConfig, error) {
	// Validate required fields
	if cfg.Name == "" {
		return jetstream.StreamConfig{}, fmt.Errorf("stream name is required")
	}
	// Subjects are required unless this is a mirror stream
	if len(cfg.Subjects) == 0 && cfg.Mirror == nil {
		return jetstream.StreamConfig{}, fmt.Errorf("at least one subject is required (unless stream is a mirror)")
	}

	// Convert internal config to JetStream config
	jsCfg := jetstream.StreamConfig{
		Name:                 cfg.Name,
		Description:          cfg.Description,
		Subjects:             cfg.Subjects,
		MaxConsumers:         cfg.MaxConsumers,
		MaxMsgs:              cfg.MaxMsgs,
		MaxBytes:             cfg.MaxBytes,
		DiscardNewPerSubject: cfg.DiscardNewPerSubject,
		MaxAge:               cfg.MaxAge,
		MaxMsgsPerSubject:    cfg.MaxMsgsPerSubject,
		MaxMsgSize:           cfg.MaxMsgSize,
		Replicas:             cfg.Replicas,
		NoAck:                cfg.NoAck,
		Duplicates:           cfg.Duplicates,
		Sealed:               cfg.Sealed,
		DenyDelete:           cfg.DenyDelete,
		DenyPurge:            cfg.DenyPurge,
		AllowRollup:          cfg.AllowRollup,
		FirstSeq:             cfg.FirstSeq,
		AllowDirect:          cfg.AllowDirect,
		MirrorDirect:         cfg.MirrorDirect,
		Metadata:             cfg.Metadata,
	}

	// Validate and convert retention policy
	switch cfg.Retention {
	case types.LimitsPolicy:
		jsCfg.Retention = jetstream.LimitsPolicy
	case types.InterestPolicy:
		jsCfg.Retention = jetstream.InterestPolicy
	case types.WorkQueuePolicy:
		jsCfg.Retention = jetstream.WorkQueuePolicy
	default:
		return jetstream.StreamConfig{}, fmt.Errorf("invalid retention policy %d: must be LimitsPolicy(0), InterestPolicy(1), or WorkQueuePolicy(2)", cfg.Retention)
	}

	// Validate and convert storage type
	switch cfg.Storage {
	case types.FileStorage:
		jsCfg.Storage = jetstream.FileStorage
	case types.MemoryStorage:
		jsCfg.Storage = jetstream.MemoryStorage
	default:
		return jetstream.StreamConfig{}, fmt.Errorf("invalid storage type %d: must be FileStorage(0) or MemoryStorage(1)", cfg.Storage)
	}

	// Validate and convert discard policy
	switch cfg.Discard {
	case types.DiscardOld:
		jsCfg.Discard = jetstream.DiscardOld
	case types.DiscardNew:
		jsCfg.Discard = jetstream.DiscardNew
	default:
		return jetstream.StreamConfig{}, fmt.Errorf("invalid discard policy %d: must be DiscardOld(0) or DiscardNew(1)", cfg.Discard)
	}

	// Validate and convert compression
	switch cfg.Compression {
	case types.NoCompression:
		jsCfg.Compression = jetstream.NoCompression
	case types.S2Compression:
		jsCfg.Compression = jetstream.S2Compression
	default:
		return jetstream.StreamConfig{}, fmt.Errorf("invalid compression %d: must be NoCompression(0) or S2Compression(1)", cfg.Compression)
	}

	// Convert complex types if present
	if cfg.Placement != nil {
		jsCfg.Placement = &jetstream.Placement{
			Cluster: cfg.Placement.Cluster,
			Tags:    cfg.Placement.Tags,
		}
	}

	if cfg.Mirror != nil {
		jsCfg.Mirror = toJetStreamStreamSource(cfg.Mirror)
	}

	if len(cfg.Sources) > 0 {
		jsCfg.Sources = make([]*jetstream.StreamSource, len(cfg.Sources))
		for i, src := range cfg.Sources {
			jsCfg.Sources[i] = toJetStreamStreamSource(src)
		}
	}

	if cfg.SubjectTransform != nil {
		jsCfg.SubjectTransform = &jetstream.SubjectTransformConfig{
			Source:      cfg.SubjectTransform.Source,
			Destination: cfg.SubjectTransform.Destination,
		}
	}

	if cfg.RePublish != nil {
		jsCfg.RePublish = &jetstream.RePublish{
			Source:      cfg.RePublish.Source,
			Destination: cfg.RePublish.Destination,
			HeadersOnly: cfg.RePublish.HeadersOnly,
		}
	}

	// Always map ConsumerLimits (zero values are meaningful - they mean "use defaults")
	jsCfg.ConsumerLimits = jetstream.StreamConsumerLimits{
		InactiveThreshold: cfg.ConsumerLimits.InactiveThreshold,
		MaxAckPending:     cfg.ConsumerLimits.MaxAckPending,
	}

	return jsCfg, nil
}

// toJetStreamStreamSource converts types.StreamSource to jetstream.StreamSource.
// It handles all nested fields including SubjectTransforms and External stream configuration.
// The Domain field is preserved for JetStream domain configuration (it sets External.APIPrefix
// automatically during stream creation).
func toJetStreamStreamSource(src *types.StreamSource) *jetstream.StreamSource {
	if src == nil {
		return nil
	}

	jsSrc := &jetstream.StreamSource{
		Name:          src.Name,
		OptStartSeq:   src.OptStartSeq,
		OptStartTime:  src.OptStartTime,
		FilterSubject: src.FilterSubject,
		Domain:        src.Domain,
	}

	if len(src.SubjectTransforms) > 0 {
		jsSrc.SubjectTransforms = make([]jetstream.SubjectTransformConfig, len(src.SubjectTransforms))
		for i, t := range src.SubjectTransforms {
			jsSrc.SubjectTransforms[i] = jetstream.SubjectTransformConfig{
				Source:      t.Source,
				Destination: t.Destination,
			}
		}
	}

	if src.External != nil {
		jsSrc.External = &jetstream.ExternalStream{
			APIPrefix:     src.External.APIPrefix,
			DeliverPrefix: src.External.DeliverPrefix,
		}
	}

	return jsSrc
}

// toJetStreamConsumerConfig converts types.ConsumerConfig to jetstream.ConsumerConfig.
// It validates ack policy, deliver policy, and replay policy before conversion,
// returning an error if invalid values are provided.
//
// Returns an error if:
//   - cfg.AckPolicy is not a valid AckPolicy enum value
//   - cfg.DeliverPolicy is not a valid DeliverPolicy enum value
//   - cfg.ReplayPolicy is not a valid ReplayPolicy enum value
func toJetStreamConsumerConfig(cfg types.ConsumerConfig) (jetstream.ConsumerConfig, error) {
	// Convert internal config to JetStream config
	jsCfg := jetstream.ConsumerConfig{
		Name:               cfg.Name,
		Description:        cfg.Description,
		OptStartSeq:        cfg.OptStartSeq,
		OptStartTime:       cfg.OptStartTime,
		AckWait:            cfg.AckWait,
		MaxDeliver:         cfg.MaxDeliver,
		BackOff:            cfg.BackOff,
		FilterSubject:      cfg.FilterSubject,
		RateLimit:          cfg.RateLimit,
		SampleFrequency:    cfg.SampleFrequency,
		MaxWaiting:         cfg.MaxWaiting,
		MaxAckPending:      cfg.MaxAckPending,
		HeadersOnly:        cfg.HeadersOnly,
		MaxRequestBatch:    cfg.MaxRequestBatch,
		MaxRequestExpires:  cfg.MaxRequestExpires,
		MaxRequestMaxBytes: cfg.MaxRequestMaxBytes,
		InactiveThreshold:  cfg.InactiveThreshold,
		Replicas:           cfg.Replicas,
		MemoryStorage:      cfg.MemoryStorage,
		FilterSubjects:     cfg.FilterSubjects,
		Metadata:           cfg.Metadata,
	}

	// Validate and convert ack policy
	switch cfg.AckPolicy {
	case types.AckExplicitPolicy:
		jsCfg.AckPolicy = jetstream.AckExplicitPolicy
	case types.AckNonePolicy:
		jsCfg.AckPolicy = jetstream.AckNonePolicy
	case types.AckAllPolicy:
		jsCfg.AckPolicy = jetstream.AckAllPolicy
	default:
		return jetstream.ConsumerConfig{}, fmt.Errorf("invalid ack policy %d: must be AckExplicitPolicy(0), AckAllPolicy(1), or AckNonePolicy(2)", cfg.AckPolicy)
	}

	// Validate and convert deliver policy
	switch cfg.DeliverPolicy {
	case types.DeliverAllPolicy:
		jsCfg.DeliverPolicy = jetstream.DeliverAllPolicy
	case types.DeliverLastPolicy:
		jsCfg.DeliverPolicy = jetstream.DeliverLastPolicy
	case types.DeliverNewPolicy:
		jsCfg.DeliverPolicy = jetstream.DeliverNewPolicy
	case types.DeliverByStartSequencePolicy:
		jsCfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
	case types.DeliverByStartTimePolicy:
		jsCfg.DeliverPolicy = jetstream.DeliverByStartTimePolicy
	case types.DeliverLastPerSubjectPolicy:
		jsCfg.DeliverPolicy = jetstream.DeliverLastPerSubjectPolicy
	default:
		return jetstream.ConsumerConfig{}, fmt.Errorf("invalid deliver policy %d: must be DeliverAllPolicy(0), DeliverLastPolicy(1), DeliverNewPolicy(2), DeliverByStartSequencePolicy(3), DeliverByStartTimePolicy(4), or DeliverLastPerSubjectPolicy(5)", cfg.DeliverPolicy)
	}

	// Validate and convert replay policy
	switch cfg.ReplayPolicy {
	case types.ReplayInstantPolicy:
		jsCfg.ReplayPolicy = jetstream.ReplayInstantPolicy
	case types.ReplayOriginalPolicy:
		jsCfg.ReplayPolicy = jetstream.ReplayOriginalPolicy
	default:
		return jetstream.ConsumerConfig{}, fmt.Errorf("invalid replay policy %d: must be ReplayInstantPolicy(0) or ReplayOriginalPolicy(1)", cfg.ReplayPolicy)
	}

	return jsCfg, nil
}
