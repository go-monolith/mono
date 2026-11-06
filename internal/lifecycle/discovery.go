package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go/micro"
	"github.com/nats-io/nuid"
)

// This file implements the discovery and stats side of the NATS services
// protocol (ADR-32, the protocol behind github.com/nats-io/nats.go/micro and
// the `nats micro ls / info / stats` CLI) for request-reply services.
//
// The framework follows the protocol rather than adopting micro.AddService:
// micro would create its own subscriptions on the service subjects and route
// requests around the worker pool, and it has its own error headers
// (Nats-Service-Error) where mono has Mono-Error. So mono answers the control
// subjects itself ($SRV.PING, $SRV.INFO, $SRV.STATS, each also suffixed with
// .<service> and .<service>.<id>) with the response types from the micro
// package, which keeps the wire format identical, and keeps its own
// subscriptions, pools and error headers for the traffic itself.
//
// One protocol service is exposed per module that owns request-reply
// services: its name is the module name, its endpoints are the module's
// request-reply services. Per-endpoint stats are the standard micro fields
// (num_requests, num_errors, last_error, processing_time,
// average_processing_time) plus mono-specific data (queue wait time, expired
// and rejected requests, pending messages and pool state).

// discoveryNameRegexp mirrors micro's service name validation.
var discoveryNameRegexp = regexp.MustCompile(`^[A-Za-z0-9\-_]+$`)

// maxLastErrorLen bounds the last_error field; CLI clients render it inline.
const maxLastErrorLen = 256

// endpointMetrics are the per-request-reply-service counters behind the
// micro endpoint stats. They are recorded on the pool worker: queue wait is
// arrival to handler start, processing is handler start to handler end, so
// the two numbers the "handlers start late" incident needs are kept apart.
type endpointMetrics struct {
	requests       atomic.Int64 // handler runs
	errors         atomic.Int64 // handler returned an error or panicked
	processingNs   atomic.Int64 // sum of handler durations
	queueWaitNs    atomic.Int64 // sum of arrival-to-start waits
	queueWaitMaxNs atomic.Int64 // largest single wait

	// Requests that expired in the queue never run, so they are not in
	// requests/queueWaitNs; their waits (the worst cases in a "handlers start
	// late" incident) are tracked separately.
	expiredQueueWaitMaxNs atomic.Int64

	mu        sync.Mutex
	lastError string
}

// recordExpired accounts the queue wait of a request that was dropped as
// expired when a worker picked it up.
func (m *endpointMetrics) recordExpired(queueWait time.Duration) {
	for {
		cur := m.expiredQueueWaitMaxNs.Load()
		if int64(queueWait) <= cur || m.expiredQueueWaitMaxNs.CompareAndSwap(cur, int64(queueWait)) {
			return
		}
	}
}

// record accounts one handler run.
func (m *endpointMetrics) record(queueWait, processing time.Duration, err error) {
	m.requests.Add(1)
	m.processingNs.Add(int64(processing))
	m.queueWaitNs.Add(int64(queueWait))
	for {
		cur := m.queueWaitMaxNs.Load()
		if int64(queueWait) <= cur || m.queueWaitMaxNs.CompareAndSwap(cur, int64(queueWait)) {
			break
		}
	}
	if err != nil {
		m.errors.Add(1)
		text := err.Error()
		if len(text) > maxLastErrorLen {
			text = strings.ToValidUTF8(text[:maxLastErrorLen], "") // never cut mid-character
		}
		m.mu.Lock()
		m.lastError = text
		m.mu.Unlock()
	}
}

// snapshot returns the standard micro fields.
func (m *endpointMetrics) snapshot() (requests, errs int64, lastError string, processing, avgProcessing, queueWait, avgQueueWait, queueWaitMax time.Duration) {
	requests = m.requests.Load()
	errs = m.errors.Load()
	processing = time.Duration(m.processingNs.Load())
	queueWait = time.Duration(m.queueWaitNs.Load())
	queueWaitMax = time.Duration(m.queueWaitMaxNs.Load())
	if requests > 0 {
		avgProcessing = processing / time.Duration(requests)
		avgQueueWait = queueWait / time.Duration(requests)
	}
	m.mu.Lock()
	lastError = m.lastError
	m.mu.Unlock()
	return
}

// EndpointData is the mono-specific part of an endpoint's stats, carried in
// the micro EndpointStats.Data field.
type EndpointData struct {
	// QueueWaitTime is the total time requests waited between arrival and
	// handler start; AverageQueueWaitTime and MaxQueueWaitTime derive from it.
	QueueWaitTime        time.Duration `json:"queue_wait_time"`
	AverageQueueWaitTime time.Duration `json:"average_queue_wait_time"`
	MaxQueueWaitTime     time.Duration `json:"max_queue_wait_time"`
	// Expired counts requests skipped because the caller's deadline had passed;
	// ExpiredQueueWaitMax is the longest such a request waited before it was
	// found expired (these waits are not part of the queue wait fields above,
	// since the handler never ran). Rejected counts requests answered
	// "overloaded".
	Expired             int64         `json:"expired"`
	ExpiredQueueWaitMax time.Duration `json:"expired_queue_wait_max"`
	Rejected            int64         `json:"rejected"`
	// PendingMsgs is the NATS client buffer; PoolActive/PoolQueued the worker pool.
	PendingMsgs int `json:"pending_msgs"`
	PoolActive  int `json:"pool_active"`
	PoolQueued  int `json:"pool_queued"`
}

// discoveryEndpoint is one request-reply service seen through the protocol.
type discoveryEndpoint struct {
	name       string
	subject    string
	queueGroup string
	record     *subscriptionRecord
}

// discoveryService is the protocol view of one module.
type discoveryService struct {
	identity    micro.ServiceIdentity
	description string
	started     time.Time
	endpoints   []*discoveryEndpoint
}

// newDiscoveryService builds the protocol service for a module. It returns an
// error for a module name the protocol cannot represent.
func newDiscoveryService(module string, cfg types.ServiceDiscoveryConfig, endpoints []*discoveryEndpoint) (*discoveryService, error) {
	if !discoveryNameRegexp.MatchString(module) {
		return nil, fmt.Errorf("module name %q cannot be a service name in the NATS services protocol (allowed: letters, digits, - and _)", module)
	}
	version := cfg.Version
	if version == "" {
		version = types.DefaultServiceDiscoveryVersion
	}
	metadata := cfg.Metadata
	if metadata == nil {
		metadata = map[string]string{} // micro always sends {}, never null
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].name < endpoints[j].name })
	return &discoveryService{
		identity: micro.ServiceIdentity{
			Name:     module,
			ID:       nuid.Next(),
			Version:  version,
			Metadata: metadata,
		},
		description: cfg.Description,
		started:     time.Now().UTC(),
		endpoints:   endpoints,
	}, nil
}

// ping is the PING response.
func (s *discoveryService) ping() micro.Ping {
	return micro.Ping{ServiceIdentity: s.identity, Type: micro.PingResponseType}
}

// info is the INFO response.
func (s *discoveryService) info() micro.Info {
	eps := make([]micro.EndpointInfo, 0, len(s.endpoints))
	for _, e := range s.endpoints {
		eps = append(eps, micro.EndpointInfo{Name: e.name, Subject: e.subject, QueueGroup: e.queueGroup, Metadata: map[string]string{}})
	}
	return micro.Info{ServiceIdentity: s.identity, Type: micro.InfoResponseType, Description: s.description, Endpoints: eps}
}

// stats is the STATS response.
func (s *discoveryService) stats() micro.Stats {
	eps := make([]*micro.EndpointStats, 0, len(s.endpoints))
	for _, e := range s.endpoints {
		es := &micro.EndpointStats{Name: e.name, Subject: e.subject, QueueGroup: e.queueGroup}
		var data EndpointData
		if r := e.record; r != nil {
			if r.metrics != nil {
				requests, errs, lastError, processing, avgProcessing, queueWait, avgQueueWait, queueWaitMax := r.metrics.snapshot()
				es.NumRequests, es.NumErrors, es.LastError = int(requests), int(errs), lastError
				es.ProcessingTime, es.AverageProcessingTime = processing, avgProcessing
				data.QueueWaitTime, data.AverageQueueWaitTime, data.MaxQueueWaitTime = queueWait, avgQueueWait, queueWaitMax
				data.ExpiredQueueWaitMax = time.Duration(r.metrics.expiredQueueWaitMaxNs.Load())
			}
			if r.reporter != nil {
				data.Expired = r.reporter.expiredTotal.Load()
				data.Rejected = r.reporter.rejectedTotal.Load()
			}
			if r.pool != nil {
				data.PoolActive, data.PoolQueued = r.pool.Stats()
			}
			if reader, ok := r.sub.(types.SubscriptionStatsReader); ok {
				if msgs, _, err := reader.Pending(); err == nil {
					data.PendingMsgs = msgs
				}
			}
		}
		if raw, err := json.Marshal(data); err == nil {
			es.Data = raw
		}
		eps = append(eps, es)
	}
	return micro.Stats{ServiceIdentity: s.identity, Type: micro.StatsResponseType, Started: s.started, Endpoints: eps}
}

// controlSubjects returns the nine subjects this service answers.
func (s *discoveryService) controlSubjects() (map[string]func() any, error) {
	out := make(map[string]func() any, 9)
	for verb, value := range map[micro.Verb]func() any{
		micro.PingVerb:  func() any { return s.ping() },
		micro.InfoVerb:  func() any { return s.info() },
		micro.StatsVerb: func() any { return s.stats() },
	} {
		for _, pair := range [][2]string{{"", ""}, {s.identity.Name, ""}, {s.identity.Name, s.identity.ID}} {
			subject, err := micro.ControlSubject(verb, pair[0], pair[1])
			if err != nil {
				return nil, err
			}
			out[subject] = value
		}
	}
	return out, nil
}

// respond answers one control request on the event bus.
func discoveryRespond(eventBus types.EventBus, logger types.Logger, msg *types.Msg, value func() any) {
	if msg.Reply == "" {
		return
	}
	payload, err := json.Marshal(value())
	if err != nil {
		logger.Error("Failed to encode service discovery response", "subject", msg.Subject, "error", err)
		return
	}
	if err := eventBus.Publish(msg.Reply, payload); err != nil {
		logger.Error("Failed to publish service discovery response", "subject", msg.Subject, "error", err)
	}
}

// subscribeDiscovery subscribes the service's control subjects. The handler
// runs on the event bus directly (no worker pool): the requests are tiny and
// every instance must answer the bare $SRV.PING for discovery to list it.
func subscribeDiscovery(eventBus types.EventBus, logger types.Logger, svc *discoveryService) ([]types.Subscription, error) {
	subjects, err := svc.controlSubjects()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(subjects))
	for subject := range subjects {
		names = append(names, subject)
	}
	sort.Strings(names) // deterministic subscription order
	subs := make([]types.Subscription, 0, len(names))
	for _, subject := range names {
		value := subjects[subject]
		sub, err := eventBus.Subscribe(subject, func(_ context.Context, msg *types.Msg) {
			discoveryRespond(eventBus, logger, msg, value)
		})
		if err != nil {
			for _, s := range subs {
				if uerr := s.Unsubscribe(); uerr != nil {
					logger.Warn("Failed to unsubscribe discovery responder", "subject", s.Subject(), "error", uerr)
				}
			}
			return nil, fmt.Errorf("failed to subscribe to %s: %w", subject, err)
		}
		subs = append(subs, sub)
	}
	return subs, nil
}
