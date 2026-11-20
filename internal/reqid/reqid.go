// Package reqid holds the rule the built-in middleware applies to a request
// ID that arrives in a message header.
//
// The header is written by whoever sent the message, which includes any
// client that can reach the NATS port, and the request ID middleware copies
// it into the handler's context and into every nested call, while the access
// log writes it into every line. Taken as it comes, a value with spaces
// forges fields of a text access log line ("x module=billing status=success"),
// one with control characters rewrites a terminal, and one of a megabyte
// makes every line a megabyte.
package reqid

// MaxLen is the longest request ID kept, in bytes. A UUID has 36, a W3C
// traceparent 55.
const MaxLen = 128

// Replacement stands for every byte of a request ID that is not allowed.
const Replacement = '_'

// Sanitize returns id cut to MaxLen bytes, with every byte other than an
// ASCII letter, a digit or one of "-_.:/=+@~" replaced by Replacement. The
// kept set covers UUIDs, ULIDs, trace IDs, base64 and prefixed IDs
// ("req_abc", "tenant/123"); it has no space, quote, control character or
// non-ASCII byte. An ID that is already clean is returned as it is, without
// allocating, so the common case costs one pass over a short string.
//
// A foreign ID is replaced rather than dropped so that the two ends of a
// call still log the same value.
func Sanitize(id string) string {
	if len(id) > MaxLen {
		id = id[:MaxLen]
	}
	for i := 0; i < len(id); i++ {
		if !Allowed(id[i]) {
			b := []byte(id)
			for j := i; j < len(b); j++ {
				if !Allowed(b[j]) {
					b[j] = Replacement
				}
			}
			return string(b)
		}
	}
	return id
}

// Allowed reports whether c may appear in a request ID.
func Allowed(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	switch c {
	case '-', '_', '.', ':', '/', '=', '+', '@', '~':
		return true
	}
	return false
}
