package types

import "time"

// RetentionPolicy defines the retention policy for a stream.
//
// RetentionPolicy controls how long messages are kept in the stream.
// The default is LimitsPolicy. Different policies suit different use cases:
// - Use LimitsPolicy for event logs and audit trails (time/size-based retention)
// - Use InterestPolicy for event streams (retain while consumers are subscribed)
// - Use WorkQueuePolicy for task queues (delete after processing)
type RetentionPolicy int

const (
	// LimitsPolicy retains messages until limits (MaxAge, MaxBytes, MaxMsgs) are reached.
	// This is the default retention policy.
	// Use this for event logs, audit trails, and compliance scenarios where you need
	// messages to persist for a specified time period or size limit regardless of consumers.
	// Messages are automatically deleted when they exceed the configured age, size, or count limits.
	// Does not require active consumers.
	LimitsPolicy RetentionPolicy = iota

	// InterestPolicy retains messages only while there are active consumers subscribed.
	// Once all consumers are removed, the stream deletes messages.
	// Use this for real-time notification systems where you don't need historical data
	// and want to save storage space by discarding messages when no one is listening.
	// This is more storage-efficient for streams where messages are only relevant while
	// there are active subscribers. Does not support adding consumers after stream creation
	// if there are no initial consumers.
	InterestPolicy

	// WorkQueuePolicy deletes messages immediately after they are acknowledged by a consumer.
	// This is designed for task/work queue patterns where each message is processed once and
	// then discarded. Use this for distributed work queues where you don't need to replay messages
	// and storage should be minimal. Perfect for async job processing, distributed task runners,
	// and fire-and-forget work patterns. Messages that are not acknowledged will be retried.
	WorkQueuePolicy
)

// StorageType defines the storage backend for a stream.
//
// StorageType controls where stream messages are persisted. The choice affects
// performance, durability, and resource usage:
// - Use FileStorage for durable, persistent storage (survives server restarts)
// - Use MemoryStorage for high-performance scenarios with acceptable data loss risk
type StorageType int

const (
	// FileStorage stores messages on disk.
	// Messages are persisted to the file system and survive server restarts.
	// Use this for production workloads requiring durability: financial transactions,
	// audit logs, compliance data, and any critical business events.
	// Performance: Slightly slower than MemoryStorage but with full durability.
	// Data loss risk: None - messages survive server crashes.
	// This is the recommended storage type for most use cases.
	FileStorage StorageType = iota

	// MemoryStorage stores messages in memory (RAM).
	// Messages are lost if the server restarts or crashes.
	// Use this for high-throughput, non-critical scenarios where you prioritize
	// performance over durability: real-time analytics, metrics collection, and
	// streams where you have multiple replicas and can tolerate occasional data loss.
	// Performance: Very fast, lowest latency.
	// Data loss risk: High - all messages lost on server restart.
	// Only use if you have failure tolerance built into your architecture.
	MemoryStorage
)

// AckPolicy defines the acknowledgement policy for a consumer.
//
// AckPolicy controls how the consumer must acknowledge received messages.
// Acknowledgements confirm to the server that a message was successfully processed.
// The policy affects reliability guarantees and processing semantics.
type AckPolicy int

const (
	// AckExplicitPolicy requires explicit acknowledgement for every message.
	// Each message must be individually acknowledged (Ack), negatively acknowledged (Nak),
	// or terminated (Term). This is the default and recommended policy for reliability.
	// Use this for critical operations where you need to ensure each message is processed exactly once.
	// Guarantees: At-least-once delivery (unacked messages are redelivered).
	// The server tracks which messages have been acked and will not deliver them again.
	// Recommended for: Payment processing, order management, audit logs, and any operation
	// where message loss is unacceptable.
	AckExplicitPolicy AckPolicy = iota

	// AckAllPolicy acknowledges a message and all messages with lower sequence numbers implicitly.
	// When you ack message N, the server also marks messages 1..N-1 as acknowledged.
	// This is more efficient than explicit acking for large batches but less granular.
	// Use this when processing batches where you want to acknowledge all previous messages
	// without individual ack calls (reduces ack overhead).
	// Guarantees: At-least-once delivery for unacked messages.
	// Example: Process 100 messages, ack only the last one to implicitly ack all 100.
	// Warning: If processing fails partway through, retried messages may overlap.
	AckAllPolicy

	// AckNonePolicy requires no acknowledgement from the consumer.
	// The server delivers messages but doesn't wait for or track acknowledgements.
	// This is fire-and-forget mode: messages are considered delivered when sent.
	// Use this for non-critical, best-effort scenarios where occasional message loss is acceptable.
	// Guarantees: At-most-once delivery (no retries, no redelivery).
	// Recommended for: Metrics, analytics, notifications where missed messages don't impact business logic.
	// Performance: Fastest option - no ack overhead.
	// Note: Unacknowledged messages may be lost if the server restarts or crashes.
	AckNonePolicy
)

// DeliverPolicy defines when a consumer should start delivering messages.
//
// DeliverPolicy controls from which point in the stream a consumer begins receiving messages.
// This determines whether you replay historical messages or start fresh with new messages.
// The default is DeliverAllPolicy (start from the beginning).
type DeliverPolicy int

const (
	// DeliverAllPolicy starts delivering from the very first message in the stream.
	// This is the default delivery policy.
	// Use this to replay the entire history: processing all messages from the beginning.
	// Recommended for: Initial data synchronization, recovery scenarios, building state from scratch.
	// Behavior: Consumer receives all messages that have ever been published, in order.
	// Warning: For large streams with millions of messages, this may be slow on first delivery.
	DeliverAllPolicy DeliverPolicy = iota

	// DeliverLastPolicy starts the consumer with only the last message in the stream.
	// Use this when you only care about the most recent state and don't need history.
	// Recommended for: Status updates, snapshots, current state consumption.
	// Behavior: Consumer skips all historical messages and only receives new messages from here on.
	// Useful for: Position tracking, latest configuration, current metrics.
	DeliverLastPolicy

	// DeliverNewPolicy only delivers messages sent after the consumer is created.
	// Use this for new consumers that should only see future messages, not historical data.
	// Recommended for: New subscribers joining a live stream, real-time notifications.
	// Behavior: All messages published before consumer creation are ignored.
	// Useful for: Alert systems, live feeds, forward-only subscribers.
	DeliverNewPolicy

	// DeliverByStartSequencePolicy starts delivering from a specific sequence number.
	// You must set ConsumerConfig.OptStartSeq to specify the starting sequence.
	// Use this when you know the exact sequence number to start from (e.g., resuming after a crash).
	// Recommended for: Recovery with known position, resumable processing.
	// Behavior: Starts at sequence N and delivers all messages from there onward.
	// Example: Resume a crashed consumer from where it last processed.
	DeliverByStartSequencePolicy

	// DeliverByStartTimePolicy starts delivering from messages created after a specific timestamp.
	// You must set ConsumerConfig.OptStartTime to specify the starting time.
	// Use this when you want to replay messages from a certain point in time.
	// Recommended for: Time-based recovery, re-processing messages from last hour/day.
	// Behavior: Starts at the first message created after the specified time.
	// Example: Replay all order events from the last 24 hours.
	DeliverByStartTimePolicy

	// DeliverLastPerSubjectPolicy starts the consumer with the last message for each subject.
	// Use this for state machines where you only need the latest update for each subject.
	// Recommended for: Key-value patterns, state snapshots with multiple subjects.
	// Behavior: For each unique subject, consumer receives only the most recent message.
	// Useful for: Loading current values of all keys, building initial state from subjects.
	// Example: Get the latest status for all 100 machines in your cluster.
	DeliverLastPerSubjectPolicy
)

// ReplayPolicy determines how the consumer should replay messages it
// already has queued in the stream.
//
// ReplayPolicy controls the rate at which historical messages are delivered to the consumer.
// This affects how quickly you can replay old messages and test with realistic timing.
// The default is ReplayInstantPolicy (as fast as possible).
type ReplayPolicy int

const (
	// ReplayInstantPolicy replays messages as fast as possible.
	// This is the default replay policy.
	// Messages are delivered at network speed, regardless of their original publish time gaps.
	// Use this for: Quick recovery, bulk data processing, testing with realistic message volumes.
	// Behavior: Consumer receives historical messages as fast as the server can send them.
	// Performance: Maximizes throughput, returns to live messages faster.
	// Warning: Tests with this policy won't accurately reflect real production timing.
	ReplayInstantPolicy ReplayPolicy = iota

	// ReplayOriginalPolicy maintains the original timing between messages.
	// The server reproduces the original timing intervals between message publishes.
	// Use this for: Realistic testing, simulating production traffic in dev/staging environments.
	// Behavior: If messages were originally published 1 second apart, they are delivered 1 second apart.
	// Recommended for: Load testing, performance testing, understanding time-sensitive logic.
	// Performance: Slower than Instant (takes real time to replay), but realistic.
	// Example: Replay 24 hours of production data in 24 real hours to test behavior.
	ReplayOriginalPolicy
)

// DiscardPolicy determines how to proceed when limits of messages or bytes
// are reached.
//
// DiscardPolicy controls what happens when a stream reaches its size or message count limits.
// You must configure limits (MaxMsgs, MaxBytes, or MaxAge) for the discard policy to take effect.
// The default is DiscardOld (remove old messages to stay within limits).
type DiscardPolicy int

const (
	// DiscardOld removes the oldest messages when stream limits are reached.
	// This is the default discard policy.
	// Use this for: Rolling windows, time-series data, circular buffers where you always keep the most recent data.
	// Behavior: When MaxMsgs or MaxBytes is exceeded, the oldest message(s) are automatically deleted.
	// Example: Keep only the last 1 million messages (when MaxMsgs=1M).
	// Recommended for: Event streams, audit logs with rolling retention, dashboards with fixed windows.
	// Guarantees: Older messages are lost first; newer messages are always kept (if possible).
	// Good for: Use when you have a rolling window of data and older data becomes irrelevant.
	DiscardOld DiscardPolicy = iota

	// DiscardNew fails to store new messages once stream limits are reached.
	// New published messages are rejected when the stream is full.
	// Use this for: Quota enforcement, preventing unbounded growth, write-once archives.
	// Behavior: When MaxMsgs or MaxBytes is reached, all new Publish calls fail until space is freed.
	// Recommended for: Strict quota systems, write-protected archives, capacity planning testing.
	// Guarantees: All existing messages are preserved; new messages may be rejected.
	// Warning: Publishers will receive errors; you must handle publish failures.
	// Good for: Immutable ledgers, write-once backups, enforcing hard limits on data growth.
	DiscardNew
)

// StoreCompression determines how messages are compressed in the stream storage.
//
// StoreCompression controls whether and how messages are compressed on disk.
// Compression trades CPU for storage space. Choose based on your hardware and workload.
// The default is NoCompression (raw storage).
type StoreCompression uint8

const (
	// NoCompression disables compression; messages are stored as-is.
	// This is the default.
	// Use this for: High-throughput scenarios, CPU-constrained environments, fast access patterns.
	// Performance: Lowest latency, no compression overhead.
	// Storage: Uses full message size (largest storage footprint).
	// Recommended for: Real-time systems, where CPU is limited and compression overhead unacceptable.
	// Good for: Metrics streams, high-frequency trading, low-latency requirements.
	NoCompression StoreCompression = iota

	// S2Compression enables S2 (Snappy) compression on messages in storage.
	// Uses the fast S2 algorithm for compression/decompression.
	// Use this for: Storage-constrained environments, cost-sensitive scenarios, batch processing.
	// Performance: Slight latency increase due to compression, but still very fast (fast codec).
	// Storage: Significantly reduced storage size (typically 50-80% reduction for JSON/text data).
	// CPU trade-off: Uses more CPU for compression during write and decompression on read.
	// Recommended for: Large archives, compliance storage, cost-optimized systems.
	// Good for: Event archives, audit logs, backup storage where speed matters but size is critical.
	// Note: S2 is faster than GZIP but slightly less compressed; good balance for most use cases.
	S2Compression
)

// Placement is used to guide placement of streams in clustered JetStream.
type Placement struct {
	// Cluster is the name of the cluster to which the stream should be
	// assigned.
	Cluster string `json:"cluster"`

	// Tags are used to match streams to servers in the cluster. A stream
	// will be assigned to a server with a matching tag.
	Tags []string `json:"tags,omitempty"`
}

// StreamSource dictates how streams can source from other streams.
type StreamSource struct {
	// Name is the name of the stream to source from.
	Name string `json:"name"`

	// OptStartSeq is the sequence number to start sourcing from.
	OptStartSeq uint64 `json:"opt_start_seq,omitempty"`

	// OptStartTime is the timestamp of messages to start sourcing from.
	OptStartTime *time.Time `json:"opt_start_time,omitempty"`

	// FilterSubject is the subject filter used to only replicate messages
	// with matching subjects.
	FilterSubject string `json:"filter_subject,omitempty"`

	// SubjectTransforms is a list of subject transforms to apply to
	// matching messages.
	//
	// Subject transforms on sources and mirrors are also used as subject
	// filters with optional transformations.
	SubjectTransforms []SubjectTransformConfig `json:"subject_transforms,omitempty"`

	// External is a configuration referencing a stream source in another
	// account or JetStream domain.
	External *ExternalStream `json:"external,omitempty"`

	// Domain is used to configure a stream source in another JetStream
	// domain. This setting will set the External field with the appropriate
	// APIPrefix. This field is not marshaled to JSON; it's used during
	// configuration setup.
	Domain string `json:"-"`
}

// ExternalStream allows you to qualify access to a stream source in another
// account.
type ExternalStream struct {
	// APIPrefix is the subject prefix that imports the other account/domain
	// $JS.API.CONSUMER.> subjects.
	APIPrefix string `json:"api"`

	// DeliverPrefix is the delivery subject to use for the push consumer.
	DeliverPrefix string `json:"deliver"`
}

// SubjectTransformConfig is for applying a subject transform (to matching
// messages) before doing anything else when a new message is received.
type SubjectTransformConfig struct {
	// Source is the subject pattern to match incoming messages against.
	Source string `json:"src"`

	// Destination is the subject pattern to remap the subject to.
	Destination string `json:"dest"`
}

// RePublish is for republishing messages once committed to a stream. The
// original subject is remapped from the subject pattern to the destination
// pattern.
type RePublish struct {
	// Source is the subject pattern to match incoming messages against.
	Source string `json:"src,omitempty"`

	// Destination is the subject pattern to republish the subject to.
	Destination string `json:"dest"`

	// HeadersOnly is a flag to indicate that only the headers should be
	// republished.
	HeadersOnly bool `json:"headers_only,omitempty"`
}

// StreamConsumerLimits are the limits for a consumer on a stream. These can
// be overridden on a per consumer basis.
type StreamConsumerLimits struct {
	// InactiveThreshold is a duration which instructs the server to clean
	// up the consumer if it has been inactive for the specified duration.
	InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`

	// MaxAckPending is a maximum number of outstanding unacknowledged
	// messages for a consumer.
	MaxAckPending int `json:"max_ack_pending,omitempty"`
}

// StreamConfig is the configuration of a JetStream stream.
// This is the framework's internal type that abstracts the underlying JetStream configuration.
type StreamConfig struct {
	// Name is the name of the stream. It is required and must be unique
	// across the JetStream account.
	//
	// Names cannot contain whitespace, ., *, >, path separators
	// (forward or backwards slash), and non-printable characters.
	Name string `json:"name"`

	// Description is an optional description of the stream.
	Description string `json:"description,omitempty"`

	// Subjects is a list of subjects that the stream is listening on.
	// Wildcards are supported. Subjects cannot be set if the stream is
	// created as a mirror.
	Subjects []string `json:"subjects,omitempty"`

	// Retention defines the message retention policy for the stream.
	// Defaults to LimitsPolicy.
	Retention RetentionPolicy `json:"retention"`

	// MaxConsumers specifies the maximum number of consumers allowed for
	// the stream. If set to 0, server default is -1 (unlimited).
	MaxConsumers int `json:"max_consumers"`

	// MaxMsgs is the maximum number of messages the stream will store.
	// After reaching the limit, stream adheres to the discard policy.
	// If not set, server default is -1 (unlimited).
	MaxMsgs int64 `json:"max_msgs"`

	// MaxBytes is the maximum total size of messages the stream will store.
	// After reaching the limit, stream adheres to the discard policy.
	// If not set, server default is -1 (unlimited).
	MaxBytes int64 `json:"max_bytes"`

	// Discard defines the policy for handling messages when the stream
	// reaches its limits in terms of number of messages or total bytes.
	Discard DiscardPolicy `json:"discard"`

	// DiscardNewPerSubject is a flag to enable discarding new messages per
	// subject when limits are reached. Requires DiscardPolicy to be
	// DiscardNew and the MaxMsgsPerSubject to be set.
	DiscardNewPerSubject bool `json:"discard_new_per_subject,omitempty"`

	// MaxAge is the maximum age of messages that the stream will retain.
	MaxAge time.Duration `json:"max_age"`

	// MaxMsgsPerSubject is the maximum number of messages per subject that
	// the stream will retain.
	MaxMsgsPerSubject int64 `json:"max_msgs_per_subject"`

	// MaxMsgSize is the maximum size of any single message in the stream.
	MaxMsgSize int32 `json:"max_msg_size,omitempty"`

	// Storage specifies the type of storage backend used for the stream
	// (file or memory).
	Storage StorageType `json:"storage"`

	// Replicas is the number of stream replicas in clustered JetStream.
	// If set to 0, server default is 1. Maximum is 5.
	Replicas int `json:"num_replicas"`

	// NoAck is a flag to disable acknowledging messages received by this
	// stream.
	//
	// If set to true, publish methods from the JetStream client will not
	// work as expected, since they rely on acknowledgements. Core NATS
	// publish methods should be used instead. Note that this will make
	// message delivery less reliable.
	NoAck bool `json:"no_ack,omitempty"`

	// Duplicates is the window within which to track duplicate messages.
	// If not set, server default is 2 minutes.
	Duplicates time.Duration `json:"duplicate_window,omitempty"`

	// Placement is used to declare where the stream should be placed via
	// tags and/or an explicit cluster name.
	Placement *Placement `json:"placement,omitempty"`

	// Mirror defines the configuration for mirroring another stream.
	Mirror *StreamSource `json:"mirror,omitempty"`

	// Sources is a list of other streams this stream sources messages from.
	Sources []*StreamSource `json:"sources,omitempty"`

	// Sealed streams do not allow messages to be published or deleted via limits or API,
	// sealed streams can not be unsealed via configuration update. Can only
	// be set on already created streams via the Update API.
	Sealed bool `json:"sealed,omitempty"`

	// DenyDelete restricts the ability to delete messages from a stream via
	// the API. Defaults to false.
	DenyDelete bool `json:"deny_delete,omitempty"`

	// DenyPurge restricts the ability to purge messages from a stream via
	// the API. Defaults to false.
	DenyPurge bool `json:"deny_purge,omitempty"`

	// AllowRollup allows the use of the Nats-Rollup header to replace all
	// contents of a stream, or subject in a stream, with a single new
	// message.
	AllowRollup bool `json:"allow_rollup_hdrs,omitempty"`

	// Compression specifies the message storage compression algorithm.
	// Defaults to NoCompression.
	Compression StoreCompression `json:"compression"`

	// FirstSeq is the initial sequence number of the first message in the
	// stream.
	FirstSeq uint64 `json:"first_seq,omitempty"`

	// SubjectTransform allows applying a transformation to matching
	// messages' subjects.
	SubjectTransform *SubjectTransformConfig `json:"subject_transform,omitempty"`

	// RePublish allows immediate republishing a message to the configured
	// subject after it's stored.
	RePublish *RePublish `json:"republish,omitempty"`

	// AllowDirect enables direct access to individual messages using direct
	// get API. Defaults to false.
	AllowDirect bool `json:"allow_direct"`

	// MirrorDirect enables direct access to individual messages from the
	// origin stream using direct get API. Defaults to false.
	MirrorDirect bool `json:"mirror_direct"`

	// ConsumerLimits defines limits of certain values that consumers can
	// set, defaults for those who don't set these settings
	ConsumerLimits StreamConsumerLimits `json:"consumer_limits,omitempty"`

	// Metadata is a set of application-defined key-value pairs for
	// associating metadata on the stream. This feature requires nats-server
	// v2.10.0 or later.
	Metadata map[string]string `json:"metadata,omitempty"`

	// Template identifies the template that manages the Stream.
	// Deprecated: This feature is no longer supported.
	Template string `json:"template_owner,omitempty"`
}

// ConsumerConfig configures a JetStream consumer.
// This is the framework's internal type that abstracts the underlying JetStream configuration.
type ConsumerConfig struct {
	// Name is an optional name for the consumer. If not set, one is
	// generated automatically. This is the preferred field for naming consumers.
	//
	// If both Name and Durable are set, they must be equal.
	//
	// Name cannot contain whitespace, ., *, >, path separators (forward or
	// backwards slash), and non-printable characters.
	Name string `json:"name,omitempty"`

	// Durable is an optional durable name for the consumer.
	//
	// Deprecated: Use Name instead. Durable is maintained for backward
	// compatibility. If both Durable and Name are set, they must be equal.
	// Unless InactiveThreshold is set, a durable consumer will not be
	// cleaned up automatically.
	//
	// Durable cannot contain whitespace, ., *, >, path separators (forward or
	// backwards slash), and non-printable characters.
	Durable string `json:"durable_name,omitempty"`

	// Description provides an optional description of the consumer.
	Description string `json:"description,omitempty"`

	// DeliverPolicy defines from which point to start delivering messages
	// from the stream. Defaults to DeliverAllPolicy.
	DeliverPolicy DeliverPolicy `json:"deliver_policy"`

	// OptStartSeq is an optional sequence number from which to start
	// message delivery. Only applicable when DeliverPolicy is set to
	// DeliverByStartSequencePolicy.
	OptStartSeq uint64 `json:"opt_start_seq,omitempty"`

	// OptStartTime is an optional time from which to start message
	// delivery. Only applicable when DeliverPolicy is set to
	// DeliverByStartTimePolicy.
	OptStartTime *time.Time `json:"opt_start_time,omitempty"`

	// AckPolicy defines the acknowledgement policy for the consumer.
	// Defaults to AckExplicitPolicy.
	AckPolicy AckPolicy `json:"ack_policy"`

	// AckWait defines how long the server will wait for an acknowledgement
	// before resending a message. If not set, server default is 30 seconds.
	AckWait time.Duration `json:"ack_wait,omitempty"`

	// MaxDeliver defines the maximum number of delivery attempts for a
	// message. Applies to any message that is re-sent due to ack policy.
	//  If not set, server default is -1 (unlimited).
	MaxDeliver int `json:"max_deliver,omitempty"`

	// BackOff specifies the optional back-off intervals for retrying
	// message delivery after a failed acknowledgement. It overrides
	// AckWait.
	//
	// BackOff only applies to messages not acknowledged in specified time,
	// not messages that were nack'ed.
	//
	// The number of intervals specified must be lower or equal to
	// MaxDeliver. If the number of intervals is lower, the last interval is
	// used for all remaining attempts.
	BackOff []time.Duration `json:"backoff,omitempty"`

	// FilterSubject can be used to filter messages delivered from the
	// stream. FilterSubject is exclusive with FilterSubjects.
	FilterSubject string `json:"filter_subject,omitempty"`

	// ReplayPolicy defines the rate at which messages are sent to the
	// consumer. If ReplayOriginalPolicy is set, messages are sent in the
	// same intervals in which they were stored on stream. This can be used
	// e.g. to simulate production traffic in development environments. If
	// ReplayInstantPolicy is set, messages are sent as fast as possible.
	// Defaults to ReplayInstantPolicy.
	ReplayPolicy ReplayPolicy `json:"replay_policy"`

	// RateLimit specifies an optional maximum rate of message delivery in
	// bits per second.
	RateLimit uint64 `json:"rate_limit_bps,omitempty"`

	// SampleFrequency is an optional frequency for sampling how often
	// acknowledgements are sampled for observability. See
	// https://docs.nats.io/running-a-nats-service/nats_admin/monitoring/monitoring_jetstream
	SampleFrequency string `json:"sample_freq,omitempty"`

	// MaxWaiting is a maximum number of pull requests waiting to be
	// fulfilled. If not set, this will inherit settings from stream's
	// ConsumerLimits or (if those are not set) from account settings.  If
	// neither are set, server default is 512.
	MaxWaiting int `json:"max_waiting,omitempty"`

	// MaxAckPending is a maximum number of outstanding unacknowledged
	// messages. Once this limit is reached, the server will suspend sending
	// messages to the consumer. If not set, server default is 1000.
	// Set to -1 for unlimited.
	MaxAckPending int `json:"max_ack_pending,omitempty"`

	// HeadersOnly indicates whether only headers of messages should be sent
	// (and no payload). Defaults to false.
	HeadersOnly bool `json:"headers_only,omitempty"`

	// MaxRequestBatch is the optional maximum batch size a single pull
	// request can make. When set with MaxRequestMaxBytes, the batch size
	// will be constrained by whichever limit is hit first.
	MaxRequestBatch int `json:"max_batch,omitempty"`

	// MaxRequestExpires is the maximum duration a single pull request will
	// wait for messages to be available to pull.
	MaxRequestExpires time.Duration `json:"max_expires,omitempty"`

	// MaxRequestMaxBytes is the optional maximum total bytes that can be
	// requested in a given batch. When set with MaxRequestBatch, the batch
	// size will be constrained by whichever limit is hit first.
	MaxRequestMaxBytes int `json:"max_bytes,omitempty"`

	// InactiveThreshold is a duration which instructs the server to clean
	// up the consumer if it has been inactive for the specified duration.
	// Durable consumers will not be cleaned up by default, but if
	// InactiveThreshold is set, they will be. If not set, this will inherit
	// settings from stream's ConsumerLimits. If neither are set, server
	// default is 5 seconds.
	//
	// A consumer is considered inactive there are not pull requests
	// received by the server (for pull consumers), or no interest detected
	// on deliver subject (for push consumers), not if there are no
	// messages to be delivered.
	InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`

	// Replicas is the number of replicas for the consumer's state.
	// If set to 0, consumers inherit the number of replicas from the stream.
	Replicas int `json:"num_replicas"`

	// MemoryStorage is a flag to force the consumer to use memory storage
	// rather than inherit the storage type from the stream.
	MemoryStorage bool `json:"mem_storage,omitempty"`

	// FilterSubjects allows filtering messages from a stream by subject.
	// This field is exclusive with FilterSubject. Requires nats-server
	// v2.10.0 or later.
	FilterSubjects []string `json:"filter_subjects,omitempty"`

	// Metadata is a set of application-defined key-value pairs for
	// associating metadata on the consumer. This feature requires
	// nats-server v2.10.0 or later.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// FetchConfig configures the fetch behavior for JetStream pull consumers.
// These settings control how messages are fetched in batches.
type FetchConfig struct {
	// BatchSize is the maximum number of messages to fetch per batch.
	// If not set, defaults to 10.
	BatchSize int `json:"batch_size,omitempty"`

	// Timeout is the maximum time to wait for messages per fetch operation.
	// If not set, defaults to 5 seconds.
	Timeout time.Duration `json:"timeout,omitempty"`
}

// StreamConsumerConfig configures a JetStream durable pull consumer service.
// It composes StreamConfig for stream settings, ConsumerConfig for consumer settings,
// and FetchConfig for fetch loop behavior.
type StreamConsumerConfig struct {
	// Stream contains the JetStream stream configuration.
	// Required: Stream.Name must be set.
	// Optional: Stream.Subjects - if empty, defaults to "services.<module>.<service>.*"
	Stream StreamConfig `json:"stream"`

	// Consumer contains the JetStream consumer configuration.
	// The consumer name will be auto-generated if not set.
	Consumer ConsumerConfig `json:"consumer"`

	// Fetch contains the fetch loop configuration for the pull consumer.
	// Optional: defaults are applied for BatchSize (10) and Timeout (5s).
	Fetch FetchConfig `json:"fetch"`
}
