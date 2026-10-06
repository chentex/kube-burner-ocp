// Copyright 2025 The Kube-burner Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package measurements

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kube-burner/kube-burner/v2/pkg/config"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements/types"
	"github.com/kube-burner/kube-burner/v2/pkg/util/fileutils"
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	dataplaneLatencyMeasurementName      = "dataplaneLatencyMeasurement"
	dataplaneLatencyQuantilesMeasurement = "dataplaneLatencyQuantilesMeasurement"
	iperfClientContainer                 = "iperf3-client"
)

// UDNCNCMode is set by the udn-cnc workload before RunWorkload to control
// how dataplane latency pairs are selected ("pair", "mesh", or "multiply").
var UDNCNCMode = "pair"

// UDNCNCMeshSize is set by the udn-cnc workload in multiply mode to indicate
// the number of CUDNs per mesh group, so iperf only runs within each group.
var UDNCNCMeshSize = 0

// UDNCNCProtocol is set by the udn-cnc workload to select the iperf3 protocol ("tcp" or "udp").
var UDNCNCProtocol = "tcp"

// UDNCNCParallelStreams is set by the udn-cnc workload to control iperf3 parallel streams (-P).
var UDNCNCParallelStreams = 1

// UDNCNCDuration is set by the udn-cnc workload to control iperf3 test duration in seconds (-t).
var UDNCNCDuration = 10

// UDNCNCReverse is set by the udn-cnc workload to run iperf3 in reverse mode (-R, server→client).
var UDNCNCReverse = false

// packet sizes run for each pair, per protocol.
var tcpPacketSizes = []string{"4K", "64K", "1M"}
var udpPacketSizes = []string{"64", "512", "1400"}

// netperfReqRespSizes: [req bytes, resp bytes] for TCP_RR tests (small/medium/large).
var netperfReqRespSizes = [][2]int{{1, 1}, {512, 512}, {8192, 8192}}

var supportedDataplaneJobTypes = []config.JobType{config.CreationJob}

type dataplaneMetric struct {
	Timestamp      time.Time `json:"timestamp"`
	MetricName     string    `json:"metricName"`
	UUID           string    `json:"uuid"`
	JobName        string    `json:"jobName,omitempty"`
	Namespace      string    `json:"namespace"`
	PeerNamespace  string    `json:"peerNamespace"`
	Protocol       string    `json:"protocol"`
	PacketSize     string    `json:"packetSize"`
	Direction      string    `json:"direction"` // "forward" (client→server) or "reverse" (server→client)
	Metadata       any       `json:"metadata,omitempty"`
	ThroughputMbps float64   `json:"throughputMbps"`
	Streams       int    `json:"streams"`
	SourceNode    string `json:"sourceNode"`
	DestNode      string `json:"destNode"`
	CrossNode     bool   `json:"crossNode"`
	// TCP iperf3-only
	Retransmits int     `json:"retransmits,omitempty"`
	MeanRTTus   float64 `json:"meanRTTus,omitempty"`
	// UDP iperf3-only
	JitterMs    float64 `json:"jitterMs,omitempty"`
	LostPercent float64 `json:"lostPercent,omitempty"`
	// netperf TCP_RR-only
	TransactionRate float64 `json:"transactionRate,omitempty"` // transactions/sec
	MinLatencyUs    float64 `json:"minLatencyUs,omitempty"`
	MaxLatencyUs    float64 `json:"maxLatencyUs,omitempty"`
	StddevLatencyUs float64 `json:"stddevLatencyUs,omitempty"`
}

type iperfJSON struct {
	End struct {
		// TCP forward
		SumSent struct {
			BitsPerSecond float64 `json:"bits_per_second"`
			Retransmits   int     `json:"retransmits"`
		} `json:"sum_sent"`
		// TCP reverse: server→client, throughput measured by client as receiver
		SumReceived struct {
			BitsPerSecond float64 `json:"bits_per_second"`
		} `json:"sum_received"`
		Streams []struct {
			Sender struct {
				MeanRTT float64 `json:"mean_rtt"` // microseconds, available in forward mode
			} `json:"sender"`
		} `json:"streams"`
		// UDP
		Sum struct {
			BitsPerSecond float64 `json:"bits_per_second"`
			JitterMs      float64 `json:"jitter_ms"`
			LostPercent   float64 `json:"lost_percent"`
		} `json:"sum"`
	} `json:"end"`
}

type dataplaneLatency struct {
	measurements.BaseMeasurement
	restConfig *rest.Config
}

type dataplaneLatencyMeasurementFactory struct {
	measurements.BaseMeasurementFactory
}

func NewDataplaneLatencyMeasurementFactory(configSpec config.Spec, measurement types.Measurement, metadata map[string]any, labelSelector string) (measurements.MeasurementFactory, error) {
	return dataplaneLatencyMeasurementFactory{
		measurements.NewBaseMeasurementFactory(configSpec, measurement, metadata, labelSelector),
	}, nil
}

func (f dataplaneLatencyMeasurementFactory) NewMeasurement(jobConfig *config.Job, clientSet kubernetes.Interface, restConfig *rest.Config, embedCfg *fileutils.EmbedConfiguration) measurements.Measurement {
	return &dataplaneLatency{
		BaseMeasurement: f.NewBaseLatency(jobConfig, clientSet, restConfig, dataplaneLatencyMeasurementName, dataplaneLatencyQuantilesMeasurement, embedCfg),
		restConfig:      restConfig,
	}
}

func (d *dataplaneLatency) Start(measurementWg *sync.WaitGroup) error {
	defer measurementWg.Done()
	d.LatencyQuantiles, d.NormLatencies = nil, nil
	d.Metrics = sync.Map{}
	return nil
}

func (d *dataplaneLatency) Collect(measurementWg *sync.WaitGroup) {
	defer measurementWg.Done()
}

// runIperfPairs serializes tests sharing the same destination (iperf3 server handles one
// client at a time) while parallelizing across destinations. All configured packet sizes
// are tested sequentially per pair.
func (d *dataplaneLatency) runIperfPairs(pairs [][2]int) {
	var wg sync.WaitGroup
	dstMu := make(map[int]*sync.Mutex)
	for _, p := range pairs {
		if _, ok := dstMu[p[1]]; !ok {
			dstMu[p[1]] = &sync.Mutex{}
		}
	}
	sizes := tcpPacketSizes
	if UDNCNCProtocol == "udp" {
		sizes = udpPacketSizes
	}
	for _, p := range pairs {
		wg.Add(1)
		go func(src, dst int) {
			defer wg.Done()
			dstMu[dst].Lock()
			defer dstMu[dst].Unlock()
			ns := fmt.Sprintf("udn-cnc-%d", src)
			peerNs := fmt.Sprintf("udn-cnc-%d", dst)
			for _, size := range sizes {
				if err := d.runIperf(ns, peerNs, UDNCNCProtocol, size); err != nil {
					log.Errorf("Data plane measurement for %s->%s [%s/%s] failed: %v", ns, peerNs, UDNCNCProtocol, size, err)
				}
			}
		}(p[0], p[1])
	}
	wg.Wait()
}

func (d *dataplaneLatency) Stop() error {
	if d.JobConfig.SkipIndexing {
		return nil
	}

	numNamespaces := d.JobConfig.JobIterations
	var pairs [][2]int

	if UDNCNCMode == "mesh" {
		for i := 0; i < numNamespaces; i++ {
			for j := i + 1; j < numNamespaces; j++ {
				pairs = append(pairs, [2]int{i, j})
			}
		}
	} else if UDNCNCMode == "multiply" {
		meshSize := UDNCNCMeshSize
		if meshSize <= 0 {
			meshSize = numNamespaces
		}
		numMeshes := numNamespaces / meshSize
		for m := 0; m < numMeshes; m++ {
			for i := 0; i < meshSize; i++ {
				for j := i + 1; j < meshSize; j++ {
					pairs = append(pairs, [2]int{m*meshSize + i, m*meshSize + j})
				}
			}
		}
	} else {
		// pair mode: each pair is independent, no shared server
		for i := 0; i < numNamespaces; i += 2 {
			pairs = append(pairs, [2]int{i, i + 1})
		}
	}

	if UDNCNCProtocol == "tcp_rr" {
		d.runNetperfPairs(pairs)
	} else {
		d.runIperfPairs(pairs)
	}

	return d.StopMeasurement(d.normalizeMetrics, d.getLatency)
}

// runNetperfPairs serializes netperf TCP_RR tests to the same destination (netserver handles
// one client at a time) while parallelizing across destinations.
func (d *dataplaneLatency) runNetperfPairs(pairs [][2]int) {
	var wg sync.WaitGroup
	dstMu := make(map[int]*sync.Mutex)
	for _, p := range pairs {
		if _, ok := dstMu[p[1]]; !ok {
			dstMu[p[1]] = &sync.Mutex{}
		}
	}
	for _, p := range pairs {
		wg.Add(1)
		go func(src, dst int) {
			defer wg.Done()
			dstMu[dst].Lock()
			defer dstMu[dst].Unlock()
			ns := fmt.Sprintf("udn-cnc-%d", src)
			peerNs := fmt.Sprintf("udn-cnc-%d", dst)
			for _, sizes := range netperfReqRespSizes {
				if err := d.runNetperf(ns, peerNs, sizes[0], sizes[1]); err != nil {
					log.Errorf("Netperf TCP_RR %s->%s [%d/%d] failed: %v", ns, peerNs, sizes[0], sizes[1], err)
				}
			}
		}(p[0], p[1])
	}
	wg.Wait()
}

func (d *dataplaneLatency) runNetperf(ns, peerNs string, reqSize, respSize int) error {
	ctx := context.Background()
	pods, err := d.ClientSet.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=client",
	})
	if err != nil {
		return fmt.Errorf("failed to list client pods in %s: %v", ns, err)
	}
	if len(pods.Items) == 0 {
		return fmt.Errorf("no client pods found in %s", ns)
	}
	pod := pods.Items[0]
	sourceNode := pod.Spec.NodeName

	serverPods, err := d.ClientSet.CoreV1().Pods(peerNs).List(ctx, metav1.ListOptions{
		LabelSelector: "app=nginx",
	})
	if err != nil {
		return fmt.Errorf("failed to list server pods in %s: %v", peerNs, err)
	}
	if len(serverPods.Items) == 0 {
		return fmt.Errorf("no server pods found in %s", peerNs)
	}
	destNode := serverPods.Items[0].Spec.NodeName

	serverDNS := fmt.Sprintf("cnc-svc.%s.svc.cluster.local", peerNs)
	duration := UDNCNCDuration
	if duration < 1 {
		duration = 10
	}
	packetSize := fmt.Sprintf("%d/%d", reqSize, respSize)
	cmd := []string{
		"netperf", "-t", "TCP_RR", "-H", serverDNS, "-l", fmt.Sprintf("%d", duration),
		"--",
		"-r", fmt.Sprintf("%d,%d", reqSize, respSize),
		"-o", "MIN_LATENCY,MEAN_LATENCY,MAX_LATENCY,STDDEV_LATENCY,TRANSACTION_RATE",
	}

	var stdout, stderr bytes.Buffer
	req := d.ClientSet.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec")
	req.VersionedParams(&corev1.PodExecOptions{
		Container: iperfClientContainer,
		Command:   cmd,
		Stdin:     false,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}, scheme.ParameterCodec)

	execRestConfig := *d.restConfig
	execRestConfig.Timeout = 0
	executor, err := remotecommand.NewSPDYExecutor(&execRestConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create executor for %s/%s: %v", ns, pod.Name, err)
	}
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		return fmt.Errorf("netperf exec in %s/%s failed: %v\nstderr: %s", ns, pod.Name, err, stderr.String())
	}

	minLat, meanLat, maxLat, stddevLat, txRate, err := parseNetperfCSV(stdout.String())
	if err != nil {
		return fmt.Errorf("failed to parse netperf output from %s/%s: %v\noutput: %s", ns, pod.Name, err, stdout.String())
	}

	metric := dataplaneMetric{
		Timestamp:       time.Now().UTC(),
		MetricName:      dataplaneLatencyMeasurementName,
		UUID:            d.Uuid,
		JobName:         d.JobConfig.Name,
		Namespace:       ns,
		PeerNamespace:   peerNs,
		Protocol:        "tcp_rr",
		PacketSize:      packetSize,
		Direction:       "forward",
		Streams:         1,
		SourceNode:      sourceNode,
		DestNode:        destNode,
		CrossNode:       sourceNode != destNode,
		Metadata:        d.Metadata,
		MeanRTTus:       meanLat,
		MinLatencyUs:    minLat,
		MaxLatencyUs:    maxLat,
		StddevLatencyUs: stddevLat,
		TransactionRate: txRate,
	}
	log.Infof("Data plane %s->%s [tcp_rr/%s]: %.0f tps, %.1f µs mean latency",
		ns, peerNs, packetSize, txRate, meanLat)
	d.Metrics.Store(fmt.Sprintf("%s->%s@tcp_rr:%s", ns, peerNs, packetSize), metric)
	return nil
}

// parseNetperfCSV extracts the five numeric fields from netperf -o output.
// netperf prints a header line then a CSV data line; we find the first all-numeric CSV row.
func parseNetperfCSV(output string) (minLat, meanLat, maxLat, stddevLat, txRate float64, err error) {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) < 5 {
			continue
		}
		vals := make([]float64, 5)
		ok := true
		for i, f := range fields[:5] {
			vals[i], err = strconv.ParseFloat(strings.TrimSpace(f), 64)
			if err != nil {
				ok = false
				break
			}
		}
		if ok {
			return vals[0], vals[1], vals[2], vals[3], vals[4], nil
		}
	}
	err = fmt.Errorf("no numeric data line found")
	return
}

func (d *dataplaneLatency) runIperf(ns, peerNs, protocol, packetSize string) error {
	ctx := context.Background()
	pods, err := d.ClientSet.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=client",
	})
	if err != nil {
		return fmt.Errorf("failed to list client pods in %s: %v", ns, err)
	}
	if len(pods.Items) == 0 {
		return fmt.Errorf("no client pods found in %s", ns)
	}
	pod := pods.Items[0]
	sourceNode := pod.Spec.NodeName

	serverPods, err := d.ClientSet.CoreV1().Pods(peerNs).List(ctx, metav1.ListOptions{
		LabelSelector: "app=nginx",
	})
	if err != nil {
		return fmt.Errorf("failed to list server pods in %s: %v", peerNs, err)
	}
	if len(serverPods.Items) == 0 {
		return fmt.Errorf("no server pods found in %s", peerNs)
	}
	destNode := serverPods.Items[0].Spec.NodeName

	serverDNS := fmt.Sprintf("cnc-svc.%s.svc.cluster.local", peerNs)
	streams := UDNCNCParallelStreams
	if streams < 1 {
		streams = 1
	}
	duration := UDNCNCDuration
	if duration < 1 {
		duration = 10
	}
	direction := "forward"
	cmd := []string{"iperf3", "-J", "-c", serverDNS, "-t", fmt.Sprintf("%d", duration), "-l", packetSize, "-P", fmt.Sprintf("%d", streams)}
	if protocol == "udp" {
		cmd = append(cmd, "-u", "-b", "100M")
	}
	if UDNCNCReverse {
		if protocol == "udp" {
			log.Warnf("iperf3 UDP reverse mode is unreliable; skipping reverse for %s->%s [%s/%s]", ns, peerNs, protocol, packetSize)
			return nil
		}
		cmd = append(cmd, "-R")
		direction = "reverse"
	}

	var stdout, stderr bytes.Buffer
	req := d.ClientSet.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec")
	req.VersionedParams(&corev1.PodExecOptions{
		Container: iperfClientContainer,
		Command:   cmd,
		Stdin:     false,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}, scheme.ParameterCodec)

	execRestConfig := *d.restConfig
	execRestConfig.Timeout = 0
	executor, err := remotecommand.NewSPDYExecutor(&execRestConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create executor for %s/%s: %v", ns, pod.Name, err)
	}
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		log.Debugf("iperf3 stdout before failure (%s/%s): %s", ns, pod.Name, stdout.String())
		return fmt.Errorf("iperf3 exec in %s/%s failed: %v\nstderr: %s", ns, pod.Name, err, stderr.String())
	}

	var result iperfJSON
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		return fmt.Errorf("failed to parse iperf3 JSON from %s/%s: %v\noutput: %s", ns, pod.Name, err, stdout.String())
	}

	metric := dataplaneMetric{
		Timestamp:     time.Now().UTC(),
		MetricName:    dataplaneLatencyMeasurementName,
		UUID:          d.Uuid,
		JobName:       d.JobConfig.Name,
		Namespace:     ns,
		PeerNamespace: peerNs,
		Protocol:      protocol,
		PacketSize:    packetSize,
		Direction:     direction,
		Streams:       streams,
		SourceNode:    sourceNode,
		DestNode:      destNode,
		CrossNode:     sourceNode != destNode,
		Metadata:      d.Metadata,
	}

	if protocol == "udp" {
		metric.ThroughputMbps = result.End.Sum.BitsPerSecond / 1e6
		metric.JitterMs = result.End.Sum.JitterMs
		metric.LostPercent = result.End.Sum.LostPercent
		log.Infof("Data plane %s->%s [udp/%s P%d]: %.2f Mbps, %.3f ms jitter, %.1f%% loss",
			ns, peerNs, packetSize, streams, metric.ThroughputMbps, metric.JitterMs, metric.LostPercent)
	} else if direction == "reverse" {
		// In reverse mode the server sends; client measures as receiver — use sum_received.
		// RTT is not available from the client side in reverse mode.
		metric.ThroughputMbps = result.End.SumReceived.BitsPerSecond / 1e6
		log.Infof("Data plane %s<-%s [tcp/%s P%d rev]: %.2f Mbps",
			ns, peerNs, packetSize, streams, metric.ThroughputMbps)
	} else {
		metric.ThroughputMbps = result.End.SumSent.BitsPerSecond / 1e6
		metric.Retransmits = result.End.SumSent.Retransmits
		// average RTT across all parallel streams
		if len(result.End.Streams) > 0 {
			var totalRTT float64
			for _, s := range result.End.Streams {
				totalRTT += s.Sender.MeanRTT
			}
			metric.MeanRTTus = totalRTT / float64(len(result.End.Streams))
		}
		log.Infof("Data plane %s->%s [tcp/%s P%d]: %.2f Mbps, %d retransmits, %.1f µs RTT",
			ns, peerNs, packetSize, streams, metric.ThroughputMbps, metric.Retransmits, metric.MeanRTTus)
	}

	d.Metrics.Store(fmt.Sprintf("%s->%s@%s:%s:P%d:%s", ns, peerNs, protocol, packetSize, streams, direction), metric)
	return nil
}

func (d *dataplaneLatency) normalizeMetrics() float64 {
	d.Metrics.Range(func(_, value any) bool {
		d.NormLatencies = append(d.NormLatencies, value)
		return true
	})
	return 0
}

func (d *dataplaneLatency) getLatency(normLatency any) map[string]float64 {
	m := normLatency.(dataplaneMetric)
	switch m.Protocol {
	case "udp":
		return map[string]float64{
			"ThroughputMbps": m.ThroughputMbps,
			"JitterMs":       m.JitterMs,
			"LostPercent":    m.LostPercent,
		}
	case "tcp_rr":
		return map[string]float64{
			"MeanLatencyUs":   m.MeanRTTus,
			"TransactionRate": m.TransactionRate,
		}
	default:
		return map[string]float64{
			"ThroughputMbps": m.ThroughputMbps,
			"MeanRTTus":      m.MeanRTTus,
		}
	}
}

func (d *dataplaneLatency) IsCompatible() bool {
	return slices.Contains(supportedDataplaneJobTypes, d.JobConfig.JobType)
}
