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
// how dataplane latency pairs are selected ("pair" or "mesh").
var UDNCNCMode = "pair"

var supportedDataplaneJobTypes = []config.JobType{config.CreationJob}

type dataplaneMetric struct {
	Timestamp      time.Time `json:"timestamp"`
	MetricName     string    `json:"metricName"`
	UUID           string    `json:"uuid"`
	JobName        string    `json:"jobName,omitempty"`
	Namespace      string    `json:"namespace"`
	PeerNamespace  string    `json:"peerNamespace"`
	Metadata       any       `json:"metadata,omitempty"`
	ThroughputMbps float64   `json:"throughputMbps"`
	Retransmits    int       `json:"retransmits"`
	MeanRTTus      float64   `json:"meanRTTus"`
}

type iperfJSON struct {
	End struct {
		SumSent struct {
			BitsPerSecond float64 `json:"bits_per_second"`
			Retransmits   int     `json:"retransmits"`
		} `json:"sum_sent"`
		Streams []struct {
			Sender struct {
				MeanRTT float64 `json:"mean_rtt"` // microseconds
			} `json:"sender"`
		} `json:"streams"`
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

func (d *dataplaneLatency) Stop() error {
	if d.JobConfig.SkipIndexing {
		return nil
	}

	numNamespaces := d.JobConfig.JobIterations
	var wg sync.WaitGroup

	// In pair mode the workload job iterates over 2N namespaces — test each pair (0→1, 2→3, ...).
	// In mesh mode the workload job iterates over N namespaces — test every unique pair (i→j, i<j).
	// We detect the mode by checking whether NUM_NAMESPACES == 2 * NUM_CNCS (pair) or NUM_CNCS == 1 (mesh).
	// Since jobIterations equals NUM_NAMESPACES in both modes, we use the object input vars stored by
	// the framework. Simpler: just check if numNamespaces is reachable as pairs (i, i+1) by odd step.
	// Instead we rely on the metadata key "mode" injected by the Go workload via AdditionalVars → Metadata.
	if UDNCNCMode == "mesh" {
		for i := 0; i < numNamespaces; i++ {
			for j := i + 1; j < numNamespaces; j++ {
				wg.Add(1)
				go func(src, dst int) {
					defer wg.Done()
					ns := fmt.Sprintf("udn-cnc-%d", src)
					peerNs := fmt.Sprintf("udn-cnc-%d", dst)
					if err := d.runIperf(ns, peerNs); err != nil {
						log.Errorf("Data plane measurement for %s->%s failed: %v", ns, peerNs, err)
					}
				}(i, j)
			}
		}
	} else {
		for i := 0; i < numNamespaces; i += 2 {
			wg.Add(1)
			go func(nsIndex int) {
				defer wg.Done()
				ns := fmt.Sprintf("udn-cnc-%d", nsIndex)
				peerNs := fmt.Sprintf("udn-cnc-%d", nsIndex+1)
				if err := d.runIperf(ns, peerNs); err != nil {
					log.Errorf("Data plane measurement for %s->%s failed: %v", ns, peerNs, err)
				}
			}(i)
		}
	}
	wg.Wait()

	return d.StopMeasurement(d.normalizeMetrics, d.getLatency)
}

func (d *dataplaneLatency) runIperf(ns, peerNs string) error {
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

	serverDNS := fmt.Sprintf("cnc-svc.%s.svc.cluster.local", peerNs)
	cmd := []string{"iperf3", "-J", "-c", serverDNS, "-t", "10"}

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

	var meanRTT float64
	if len(result.End.Streams) > 0 {
		meanRTT = result.End.Streams[0].Sender.MeanRTT
	}

	metric := dataplaneMetric{
		Timestamp:      time.Now().UTC(),
		MetricName:     dataplaneLatencyMeasurementName,
		UUID:           d.Uuid,
		JobName:        d.JobConfig.Name,
		Namespace:      ns,
		PeerNamespace:  peerNs,
		Metadata:       d.Metadata,
		ThroughputMbps: result.End.SumSent.BitsPerSecond / 1e6,
		Retransmits:    result.End.SumSent.Retransmits,
		MeanRTTus:      meanRTT,
	}
	d.Metrics.Store(fmt.Sprintf("%s->%s", ns, peerNs), metric)
	log.Infof("Data plane %s->%s: %.2f Mbps, %d retransmits, %.1f µs RTT",
		ns, peerNs, metric.ThroughputMbps, metric.Retransmits, metric.MeanRTTus)
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
	return map[string]float64{
		"ThroughputMbps": m.ThroughputMbps,
		"MeanRTTus":      m.MeanRTTus,
	}
}

func (d *dataplaneLatency) IsCompatible() bool {
	return slices.Contains(supportedDataplaneJobTypes, d.JobConfig.JobType)
}
