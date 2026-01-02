// Package main provides a tool to parse Go benchmark JSON output and convert it
// to a structured JSON format for benchmark result reporting.
//
// Usage:
//
//	go test -bench=. -benchmem -json ./bench/ | go run ./bench/cmd/benchparse
//
// The tool auto-detects mode from benchmark names (BenchmarkInProcess* or BenchmarkSocket*).
// It reads from stdin and writes to mono_benchmark_result.json.
//
// Framework benchmarks are processed separately with target values and pass/fail indicators.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// TestEvent represents a single event from `go test -json` output.
type TestEvent struct {
	Action  string `json:"Action"`
	Output  string `json:"Output"`
	Package string `json:"Package"`
}

// BenchmarkResult holds a single benchmark result for JSON output.
type BenchmarkResult struct {
	Name        string  `json:"name"`
	Mode        string  `json:"mode"`
	Iterations  int     `json:"iterations"`
	NsPerOp     float64 `json:"ns_per_op"`
	BytesPerOp  int64   `json:"bytes_per_op,omitempty"`
	AllocsPerOp int64   `json:"allocs_per_op,omitempty"`
	MBPerSec    float64 `json:"mb_per_sec,omitempty"`
}

// FrameworkBenchmarkTarget defines performance targets for framework benchmarks.
// Values are based on foundation.md specifications.
type FrameworkBenchmarkTarget struct {
	DurationTargetMs float64 // Target duration in milliseconds (0 = not applicable)
	MemoryTargetMB   float64 // Target memory in MB (0 = not applicable)
	ThroughputTarget float64 // Target throughput in msgs/sec (0 = not applicable)
}

// frameworkTargetEntry pairs a benchmark pattern with its target.
type frameworkTargetEntry struct {
	pattern string
	target  FrameworkBenchmarkTarget
}

// frameworkTargets defines benchmark patterns and their performance targets.
// Ordered from most specific to least specific to ensure deterministic matching.
// Targets are from docs/spec/foundation.md:
// - Framework startup: < 10ms for 10 modules
// - Memory footprint: < 20MB base overhead
// - Message throughput: > 40,000 msgs/sec
var frameworkTargets = []frameworkTargetEntry{
	{"BenchmarkFrameworkStartupWithDependencies/", FrameworkBenchmarkTarget{DurationTargetMs: 10}},
	{"BenchmarkFrameworkStartup/", FrameworkBenchmarkTarget{DurationTargetMs: 10}},
	{"BenchmarkFrameworkMemoryFootprint/", FrameworkBenchmarkTarget{MemoryTargetMB: 20}},
	{"BenchmarkFrameworkThroughput", FrameworkBenchmarkTarget{ThroughputTarget: 40000}},
}

// FrameworkBenchmarkResult holds a framework benchmark result with targets and pass/fail status.
type FrameworkBenchmarkResult struct {
	Name             string `json:"name"`
	Duration         string `json:"duration,omitempty"`          // Formatted duration (e.g., "32.65ms")
	Memory           string `json:"memory,omitempty"`            // Formatted memory (e.g., "0.06MB")
	Throughput       string `json:"throughput,omitempty"`        // Formatted throughput (e.g., "104653 msgs/sec")
	TargetDuration   string `json:"target_duration,omitempty"`   // Target duration (e.g., "2000ms")
	TargetMemory     string `json:"target_memory,omitempty"`     // Target memory (e.g., "50MB")
	TargetThroughput string `json:"target_throughput,omitempty"` // Target throughput (e.g., "10000 msgs/sec")
	DurationPassed   *bool  `json:"duration_passed,omitempty"`   // nil if not applicable
	MemoryPassed     *bool  `json:"memory_passed,omitempty"`     // nil if not applicable
	ThroughputPassed *bool  `json:"throughput_passed,omitempty"` // nil if not applicable
}

// BenchmarkResults holds all benchmark results for JSON output.
type BenchmarkResults struct {
	Timestamp        string                     `json:"timestamp"`
	Results          []BenchmarkResult          `json:"results"`
	FrameworkResults []FrameworkBenchmarkResult `json:"framework_results,omitempty"`
}

// coreRegex matches the core parts of benchmark output (name, iterations, ns/op).
var coreRegex = regexp.MustCompile(`^(Benchmark\S+)\s+(\d+)\s+([\d.]+)\s+ns/op`)

// metricRegex matches value+unit pairs in benchmark output.
var metricRegex = regexp.MustCompile(`([\d.]+)\s+([\w/_]+)`)

func main() {
	// Parse benchmark output from stdin
	scanner := bufio.NewScanner(os.Stdin)
	var results []BenchmarkResult
	var frameworkResults []FrameworkBenchmarkResult

	for scanner.Scan() {
		var event TestEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			continue
		}
		if event.Action != "output" {
			continue
		}

		line := strings.TrimSpace(event.Output)
		result, customMetrics := parseBenchmarkLine(line)
		if result == nil {
			continue
		}

		results = append(results, *result)

		// Process framework benchmarks separately
		if fwResult := processFrameworkBenchmark(*result, customMetrics); fwResult != nil {
			frameworkResults = append(frameworkResults, *fwResult)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}

	output := BenchmarkResults{
		Timestamp:        time.Now().UTC().Format(time.RFC3339),
		Results:          results,
		FrameworkResults: frameworkResults,
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling output: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile("mono_benchmark_result.json", data, 0600); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(data))
}

// parseBenchmarkLine parses a benchmark output line and returns the result plus custom metrics.
func parseBenchmarkLine(line string) (*BenchmarkResult, map[string]float64) {
	coreMatches := coreRegex.FindStringSubmatch(line)
	if coreMatches == nil {
		return nil, nil
	}

	benchName := coreMatches[1]
	iterations, err := strconv.Atoi(coreMatches[2])
	if err != nil {
		return nil, nil
	}

	nsPerOp, err := strconv.ParseFloat(coreMatches[3], 64)
	if err != nil {
		return nil, nil
	}

	result := &BenchmarkResult{
		Name:       benchName,
		Mode:       detectMode(benchName),
		Iterations: iterations,
		NsPerOp:    nsPerOp,
	}

	// Parse all value+unit pairs after ns/op
	customMetrics := make(map[string]float64)
	restOfLine := line[len(coreMatches[0]):]
	metricMatches := metricRegex.FindAllStringSubmatch(restOfLine, -1)

	for _, m := range metricMatches {
		value, parseErr := strconv.ParseFloat(m[1], 64)
		if parseErr != nil {
			continue
		}
		unit := m[2]

		switch unit {
		case "MB/s":
			result.MBPerSec = value
		case "B/op":
			result.BytesPerOp = int64(value)
		case "allocs/op":
			result.AllocsPerOp = int64(value)
		default:
			// Custom metrics like MB_alloc_delta, MB_heap_delta
			customMetrics[unit] = value
		}
	}

	return result, customMetrics
}

// detectMode determines the benchmark mode from the benchmark name.
func detectMode(name string) string {
	switch {
	case strings.HasPrefix(name, "BenchmarkInProcess"):
		return "in_process"
	case strings.HasPrefix(name, "BenchmarkSocket"):
		return "socket"
	case strings.HasPrefix(name, "BenchmarkFramework"):
		return "framework"
	case strings.HasPrefix(name, "BenchmarkMultiModule"):
		return "multi_module"
	default:
		return "unknown"
	}
}

// isFrameworkBenchmark checks if a benchmark is a Framework benchmark.
func isFrameworkBenchmark(name string) bool {
	return strings.HasPrefix(name, "BenchmarkFramework")
}

// getFrameworkTarget finds the target for a benchmark name.
// Iterates through patterns in order (most specific first) for deterministic matching.
func getFrameworkTarget(benchName string) (FrameworkBenchmarkTarget, bool) {
	for _, entry := range frameworkTargets {
		if strings.HasPrefix(benchName, entry.pattern) {
			return entry.target, true
		}
	}
	return FrameworkBenchmarkTarget{}, false
}

// nsToMs converts nanoseconds to milliseconds.
func nsToMs(ns float64) float64 {
	return ns / 1_000_000
}

// msgsPerSecFromNs calculates messages per second from ns/op.
func msgsPerSecFromNs(nsPerOp float64) float64 {
	if nsPerOp <= 0 {
		return 0
	}
	return 1_000_000_000 / nsPerOp
}

// boolPtr returns a pointer to a bool value.
func boolPtr(b bool) *bool {
	return &b
}

// processFrameworkBenchmark converts a BenchmarkResult to FrameworkBenchmarkResult with targets.
// Returns nil if the benchmark is not a framework benchmark or has no metrics to report.
func processFrameworkBenchmark(br BenchmarkResult, customMetrics map[string]float64) *FrameworkBenchmarkResult {
	if !isFrameworkBenchmark(br.Name) {
		return nil
	}

	// Check if this is a known benchmark type
	isStartup := strings.Contains(br.Name, "Startup")
	isMemory := strings.Contains(br.Name, "MemoryFootprint")
	isThroughput := strings.Contains(br.Name, "Throughput")

	// Skip unknown framework benchmark types (no metrics to extract)
	if !isStartup && !isMemory && !isThroughput {
		return nil
	}

	target, hasTarget := getFrameworkTarget(br.Name)
	result := &FrameworkBenchmarkResult{
		Name: br.Name,
	}

	// Handle startup benchmarks (duration target)
	if isStartup {
		durationMs := nsToMs(br.NsPerOp)
		result.Duration = fmt.Sprintf("%.2fms", durationMs)

		if hasTarget && target.DurationTargetMs > 0 {
			result.TargetDuration = fmt.Sprintf("%.0fms", target.DurationTargetMs)
			result.DurationPassed = boolPtr(durationMs <= target.DurationTargetMs)
		}
	}

	// Handle memory benchmarks (memory target using MB_heap_delta)
	if isMemory {
		// Use MB_heap_delta as the primary memory metric (live heap objects)
		if heapMB, ok := customMetrics["MB_heap_delta"]; ok {
			result.Memory = fmt.Sprintf("%.2fMB", heapMB)

			if hasTarget && target.MemoryTargetMB > 0 {
				result.TargetMemory = fmt.Sprintf("%.0fMB", target.MemoryTargetMB)
				result.MemoryPassed = boolPtr(heapMB <= target.MemoryTargetMB)
			}
		} else if allocMB, ok := customMetrics["MB_alloc_delta"]; ok {
			// Fallback to MB_alloc_delta if MB_heap_delta is not available
			result.Memory = fmt.Sprintf("%.2fMB", allocMB)

			if hasTarget && target.MemoryTargetMB > 0 {
				result.TargetMemory = fmt.Sprintf("%.0fMB", target.MemoryTargetMB)
				result.MemoryPassed = boolPtr(allocMB <= target.MemoryTargetMB)
			}
		}
	}

	// Handle throughput benchmarks (msgs/sec target)
	if isThroughput {
		msgsPerSec := msgsPerSecFromNs(br.NsPerOp)
		result.Throughput = fmt.Sprintf("%.0f msgs/sec", msgsPerSec)

		if hasTarget && target.ThroughputTarget > 0 {
			result.TargetThroughput = fmt.Sprintf("%.0f msgs/sec", target.ThroughputTarget)
			// Throughput: higher is better, so actual >= target means PASS
			result.ThroughputPassed = boolPtr(msgsPerSec >= target.ThroughputTarget)
		}
	}

	return result
}
