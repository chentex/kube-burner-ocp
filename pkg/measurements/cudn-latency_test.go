package measurements

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/kube-burner/kube-burner/v2/pkg/config"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func newTestCudn() *cudnLatency {
	job := config.Job{Name: "test-job"}
	return &cudnLatency{
		BaseMeasurement: measurements.BaseMeasurement{JobConfig: &job},
	}
}

func makeCUDN(t *testing.T, name, topology, created, netCreated, netAlloc string) *unstructured.Unstructured {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetName(name)
	_ = unstructured.SetNestedField(obj.Object, created, "metadata", "creationTimestamp")
	if topology != "" {
		_ = unstructured.SetNestedField(obj.Object, topology, "spec", "network", "topology")
	}
	conds := []any{}
	if netCreated != "" {
		conds = append(conds, map[string]any{"type": "NetworkCreated", "status": "True", "lastTransitionTime": netCreated})
	}
	if netAlloc != "" {
		conds = append(conds, map[string]any{"type": "NetworkAllocationSucceeded", "status": "True", "lastTransitionTime": netAlloc})
	}
	if len(conds) > 0 {
		_ = unstructured.SetNestedField(obj.Object, conds, "status", "conditions")
	}
	return obj
}

func metricFrom(t *testing.T, c *cudnLatency, name string) cudnMetric {
	t.Helper()
	v, ok := c.Metrics.Load(name)
	if !ok {
		t.Fatalf("metric for %s not stored", name)
	}
	return v.(cudnMetric)
}

func TestCudnLatencyL2NetworkCreatedOnly(t *testing.T) {
	c := newTestCudn()
	// L2: both conditions present at ADD (same instant, as observed live)
	c.handleAdd(makeCUDN(t, "l2-0", "Layer2",
		"2026-11-10T00:00:00Z", "2026-11-10T00:00:10Z", "2026-11-10T00:00:10Z"))
	m := metricFrom(t, c, "l2-0")
	if m.Topology != "Layer2" {
		t.Fatalf("topology = %q, want Layer2", m.Topology)
	}
	if m.NetworkCreatedLatency != 10000 {
		t.Fatalf("NetworkCreatedLatency = %d, want 10000", m.NetworkCreatedLatency)
	}
	if m.NetworkAllocationLatency != -1 {
		t.Fatalf("NetworkAllocationLatency = %d, want -1 for L2", m.NetworkAllocationLatency)
	}
	// getLatency must not emit the allocation metric
	lat := c.getLatency(m)
	if _, ok := lat["NetworkAllocationLatency"]; ok {
		t.Fatal("NetworkAllocationLatency must be omitted for L2")
	}
	if lat["NetworkCreatedLatency"] != 10000 {
		t.Fatalf("quantile NetworkCreatedLatency = %v, want 10000", lat["NetworkCreatedLatency"])
	}
}

func TestCudnLatencyL3TwoPhase(t *testing.T) {
	c := newTestCudn()
	// L3: ADD with only NetworkCreated; NetworkAllocationSucceeded comes later
	c.handleAdd(makeCUDN(t, "l3-0", "Layer3",
		"2026-11-10T00:00:00Z", "2026-11-10T00:00:10Z", ""))
	m := metricFrom(t, c, "l3-0")
	if m.NetworkCreatedLatency != 10000 {
		t.Fatalf("NetworkCreatedLatency = %d, want 10000", m.NetworkCreatedLatency)
	}
	if m.NetworkAllocationLatency != -1 {
		t.Fatalf("NetworkAllocationLatency = %d before alloc, want -1", m.NetworkAllocationLatency)
	}

	// UPDATE: allocation succeeds 40s after creation
	c.handleUpdate(nil, makeCUDN(t, "l3-0", "Layer3",
		"2026-11-10T00:00:00Z", "2026-11-10T00:00:10Z", "2026-11-10T00:00:40Z"))
	m = metricFrom(t, c, "l3-0")
	if m.NetworkCreatedLatency != 10000 {
		t.Fatalf("NetworkCreatedLatency after update = %d, want 10000", m.NetworkCreatedLatency)
	}
	if m.NetworkAllocationLatency != 40000 {
		t.Fatalf("NetworkAllocationLatency after update = %d, want 40000", m.NetworkAllocationLatency)
	}
	lat := c.getLatency(m)
	if lat["NetworkAllocationLatency"] != 40000 {
		t.Fatalf("quantile NetworkAllocationLatency = %v, want 40000", lat["NetworkAllocationLatency"])
	}

	// Further updates must not clobber recorded values
	c.handleUpdate(nil, makeCUDN(t, "l3-0", "Layer3",
		"2026-11-10T00:00:00Z", "2026-11-10T00:00:10Z", "2026-11-10T00:01:30Z"))
	m = metricFrom(t, c, "l3-0")
	if m.NetworkAllocationLatency != 40000 || m.NetworkCreatedLatency != 10000 {
		t.Fatalf("latencies clobbered by later update: %+v", m)
	}
}

func TestCudnLatencyL3AddAlreadyReady(t *testing.T) {
	c := newTestCudn()
	c.handleAdd(makeCUDN(t, "l3-1", "Layer3",
		"2026-11-10T00:00:00Z", "2026-11-10T00:00:10Z", "2026-11-10T00:00:55Z"))
	m := metricFrom(t, c, "l3-1")
	if m.NetworkCreatedLatency != 10000 || m.NetworkAllocationLatency != 55000 {
		t.Fatalf("add with both conditions: created=%d alloc=%d, want 10000/55000", m.NetworkCreatedLatency, m.NetworkAllocationLatency)
	}
}

func TestCudnLatencyUpdateWithoutAddIgnored(t *testing.T) {
	c := newTestCudn()
	// update for a name we never saw (e.g. pre-existing object before watch)
	c.handleUpdate(nil, makeCUDN(t, "ghost", "Layer2",
		"2026-11-10T00:00:00Z", "2026-11-10T00:00:10Z", ""))
	if _, ok := c.Metrics.Load("ghost"); ok {
		t.Fatal("update for unknown CUDN must not create a metric")
	}
}

func TestCudnLatencyNormalizeExcludesUncreated(t *testing.T) {
	c := newTestCudn()
	c.handleAdd(makeCUDN(t, "ok", "Layer2", "2026-11-10T00:00:00Z", "2026-11-10T00:00:10Z", ""))
	// never reached NetworkCreated (still False/absent)
	c.handleAdd(makeCUDN(t, "stuck", "Layer2", "2026-11-10T00:00:00Z", "", ""))
	_ = c.normalizeMetrics()
	if len(c.NormLatencies) != 1 {
		t.Fatalf("NormLatencies = %d, want 1 (stuck excluded)", len(c.NormLatencies))
	}
}

func TestCudnLatencyMissingLttFallback(t *testing.T) {
	c := newTestCudn()
	c.handleAdd(makeCUDN(t, "l2-1", "Layer2", "2026-11-10T00:00:00Z", "", ""))
	obj := makeCUDN(t, "l2-1", "Layer2", "2026-11-10T00:00:00Z", "", "")
	_ = unstructured.SetNestedField(obj.Object,
		[]any{map[string]any{"type": "NetworkCreated", "status": "True"}}, "status", "conditions")
	before := time.Now()
	c.handleUpdate(nil, obj)
	after := time.Now()
	m := metricFrom(t, c, "l2-1")
	if m.NetworkCreatedLatency < 0 {
		t.Fatalf("expected fallback to now(), got %d", m.NetworkCreatedLatency)
	}
	ref := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	if m.NetworkCreatedLatency < int(before.Sub(ref).Milliseconds()) ||
		m.NetworkCreatedLatency > int(after.Sub(ref).Milliseconds()) {
		t.Fatalf("fallback latency %d out of [before, after]", m.NetworkCreatedLatency)
	}
}

func TestCudnLatencyJSONTags(t *testing.T) {
	m := cudnMetric{
		Name:                     "x",
		Topology:                 "Layer3",
		NetworkCreatedLatency:    10,
		NetworkAllocationLatency: -1,
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var m2 cudnMetric
	if err := json.Unmarshal(b, &m2); err != nil {
		t.Fatal(err)
	}
	if m2.NetworkCreatedLatency != 10 || m2.NetworkAllocationLatency != -1 || m2.Topology != "Layer3" {
		t.Fatalf("round-trip mismatch: %+v", m2)
	}
	var raw map[string]any
	_ = json.Unmarshal(b, &raw)
	if _, ok := raw["networkCreatedLatency"]; !ok {
		t.Fatalf("missing networkCreatedLatency key: %s", b)
	}
	if _, ok := raw["networkAllocationLatency"]; !ok {
		t.Fatalf("missing networkAllocationLatency key: %s", b)
	}
	if _, ok := raw["networkAllocLatency"]; ok {
		t.Fatalf("stale networkAllocLatency key still present: %s", b)
	}
}
