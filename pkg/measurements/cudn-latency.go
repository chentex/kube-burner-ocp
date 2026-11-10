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
	"slices"
	"sync"
	"time"

	"github.com/kube-burner/kube-burner/v2/pkg/config"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements/types"
	"github.com/kube-burner/kube-burner/v2/pkg/util/fileutils"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	cudnLatencyMeasurementName      = "cudnLatencyMeasurement"
	cudnLatencyQuantilesMeasurement = "cudnLatencyQuantilesMeasurement"
	// conditionTypeNetworkCreated is True when the UDN controller created the
	// NetworkAttachmentDefinition. Readiness signal for both Layer2 and Layer3.
	conditionTypeNetworkCreated = "NetworkCreated"
	// conditionTypeNetworkAllocationSucceeded is True when node allocation
	// finished for all synced nodes. Only meaningful for Layer3 (per-node
	// subnets); for Layer2 ovn-k allocates only network IDs/tunnel keys at
	// controller init, so it transitions together with NetworkCreated.
	conditionTypeNetworkAllocationSucceeded = "NetworkAllocationSucceeded"
	// topologyLayer3 matches spec.network.topology for Layer3 CUDNs.
	topologyLayer3 = "Layer3"
)

var (
	supportedCudnLatencyJobTypes = []config.JobType{config.CreationJob}
	cudnGVRForLatency            = schema.GroupVersionResource{
		Group:    "k8s.ovn.org",
		Version:  "v1",
		Resource: "clusteruserdefinednetworks",
	}
)

type cudnMetric struct {
	Timestamp                time.Time `json:"timestamp"`
	MetricName               string    `json:"metricName"`
	UUID                     string    `json:"uuid"`
	JobName                  string    `json:"jobName,omitempty"`
	Name                     string    `json:"cudnName"`
	Metadata                 any       `json:"metadata,omitempty"`
	Topology                 string    `json:"topology,omitempty"`
	NetworkCreatedLatency    int       `json:"networkCreatedLatency"`
	NetworkAllocationLatency int       `json:"networkAllocationLatency"`
}

type cudnLatency struct {
	measurements.BaseMeasurement
	stopCh        chan struct{}
	dynamicClient dynamic.Interface
}

type cudnLatencyMeasurementFactory struct {
	measurements.BaseMeasurementFactory
}

func NewCudnLatencyMeasurementFactory(configSpec config.Spec, measurement types.Measurement, metadata map[string]any, labelSelector string) (measurements.MeasurementFactory, error) {
	return cudnLatencyMeasurementFactory{
		measurements.NewBaseMeasurementFactory(configSpec, measurement, metadata, labelSelector),
	}, nil
}

func (clmf cudnLatencyMeasurementFactory) NewMeasurement(jobConfig *config.Job, clientSet kubernetes.Interface, restConfig *rest.Config, embedCfg *fileutils.EmbedConfiguration) measurements.Measurement {
	return &cudnLatency{
		BaseMeasurement: clmf.NewBaseLatency(jobConfig, clientSet, restConfig, cudnLatencyMeasurementName, cudnLatencyQuantilesMeasurement, embedCfg),
		dynamicClient:   dynamic.NewForConfigOrDie(restConfig),
	}
}

func (c *cudnLatency) handleAdd(obj any) {
	cudn := obj.(*unstructured.Unstructured)
	cudnName, found, _ := unstructured.NestedString(cudn.UnstructuredContent(), "metadata", "name")
	if !found || cudnName == "" {
		log.Error("CUDN object missing metadata.name, skipping")
		return
	}
	ts, _, _ := unstructured.NestedString(cudn.UnstructuredContent(), "metadata", "creationTimestamp")
	if !found || ts == "" {
		log.Errorf("CUDN %s missing creationTimestamp, skipping", cudnName)
		return
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		log.Errorf("Error parsing CUDN %s creation timestamp: %v", cudnName, err)
		return
	}

	// Store creation timestamp; latencies start pending and are filled in as
	// conditions transition (or immediately, if the informer delivered an
	// object that was already ready).
	m := cudnMetric{
		Name:                     cudnName,
		Timestamp:                t.UTC(),
		MetricName:               cudnLatencyMeasurementName,
		UUID:                     c.Uuid,
		Metadata:                 c.Metadata,
		JobName:                  c.JobConfig.Name,
		Topology:                 cudnTopology(cudn),
		NetworkCreatedLatency:    -1, // Not yet ready
		NetworkAllocationLatency: -1, // Not yet ready
	}
	stored, _ := c.Metrics.LoadOrStore(cudnName, m)
	c.fillTransitionTimes(cudn, stored.(cudnMetric))
	log.Debugf("CUDN %s (%s) created at %v, waiting for NetworkCreated", cudnName, m.Topology, t.UTC())
}

func (c *cudnLatency) handleUpdate(oldObj, newObj any) {
	cudn := newObj.(*unstructured.Unstructured)
	cudnName, _, _ := unstructured.NestedString(cudn.UnstructuredContent(), "metadata", "name")

	val, ok := c.Metrics.Load(cudnName)
	if !ok {
		return
	}
	m := val.(cudnMetric)
	if m.NetworkCreatedLatency >= 0 && (m.Topology != topologyLayer3 || m.NetworkAllocationLatency >= 0) {
		return // all latencies relevant to this topology already recorded
	}
	c.fillTransitionTimes(cudn, m)
}

// fillTransitionTimes fills the pending latency fields of m from the
// transition times of the CUDN conditions:
//
//   - NetworkCreated (both topologies): creation -> NAD created
//   - NetworkAllocationSucceeded (Layer3 only): creation -> all node subnets
//     allocated. Skipped for Layer2, where ovn-k allocates only network IDs
//     and tunnel keys at controller init, so it transitions together with
//     NetworkCreated and carries no additional signal.
//
// The updated metric is stored back; pending (-1) fields are left untouched.
func (c *cudnLatency) fillTransitionTimes(cudn *unstructured.Unstructured, m cudnMetric) {
	if m.NetworkCreatedLatency < 0 {
		if t, ok := getConditionTransitionTime(cudn, conditionTypeNetworkCreated); ok {
			latency := t.Sub(m.Timestamp).Milliseconds()
			m.NetworkCreatedLatency = int(latency)
			log.Debugf("CUDN %s NetworkCreated after %dms", m.Name, latency)
		}
	}
	if m.NetworkAllocationLatency < 0 && m.Topology == topologyLayer3 {
		if t, ok := getConditionTransitionTime(cudn, conditionTypeNetworkAllocationSucceeded); ok {
			latency := t.Sub(m.Timestamp).Milliseconds()
			m.NetworkAllocationLatency = int(latency)
			log.Debugf("CUDN %s NetworkAllocationSucceeded after %dms", m.Name, latency)
		}
	}
	c.Metrics.Store(m.Name, m)
}

// cudnTopology returns spec.network.topology ("" when unset).
func cudnTopology(cudn *unstructured.Unstructured) string {
	topology, _, _ := unstructured.NestedString(cudn.UnstructuredContent(), "spec", "network", "topology")
	return topology
}

// getConditionTransitionTime returns the lastTransitionTime of the given
// True status condition, and true if found.
func getConditionTransitionTime(cudn *unstructured.Unstructured, condType string) (time.Time, bool) {
	conditions, found, err := unstructured.NestedSlice(cudn.UnstructuredContent(), "status", "conditions")
	if err != nil || !found {
		return time.Time{}, false
	}
	for _, c := range conditions {
		condition, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		condTypeField, _, _ := unstructured.NestedString(condition, "type")
		condStatus, _, _ := unstructured.NestedString(condition, "status")
		if condTypeField == condType && condStatus == "True" {
			ltt, _, _ := unstructured.NestedString(condition, "lastTransitionTime")
			if t, err := time.Parse(time.RFC3339, ltt); err == nil {
				return t, true
			}
			// Fall back to current time if lastTransitionTime is missing
			log.Warnf("CUDN %s: %s=True but missing lastTransitionTime, using current time", cudn.GetName(), condType)
			return time.Now().UTC(), true
		}
	}
	return time.Time{}, false
}

func (c *cudnLatency) Start(measurementWg *sync.WaitGroup) error {
	defer measurementWg.Done()

	c.LatencyQuantiles, c.NormLatencies = nil, nil
	c.Metrics = sync.Map{}

	if c.JobConfig.SkipIndexing {
		return nil
	}

	c.stopCh = make(chan struct{})
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(c.dynamicClient, 0, "", nil)
	cudnInformer := factory.ForResource(cudnGVRForLatency).Informer()
	cudnInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.handleAdd,
		UpdateFunc: c.handleUpdate,
	})

	log.Infof("Starting CUDN latency watcher for job %s", c.JobConfig.Name)
	factory.Start(c.stopCh)
	factory.WaitForCacheSync(c.stopCh)
	return nil
}

func (c *cudnLatency) Collect(measurementWg *sync.WaitGroup) {
	defer measurementWg.Done()
}

func (c *cudnLatency) Stop() error {
	if c.JobConfig.SkipIndexing {
		return nil
	}
	close(c.stopCh)
	return c.StopMeasurement(c.normalizeMetrics, c.getLatency)
}

func (c *cudnLatency) normalizeMetrics() float64 {
	c.Metrics.Range(func(key, value any) bool {
		m := value.(cudnMetric)
		if m.NetworkCreatedLatency < 0 {
			log.Warnf("CUDN %s never reached NetworkCreated=True, excluding from latency metrics", m.Name)
			return true
		}
		if m.Topology == topologyLayer3 && m.NetworkAllocationLatency < 0 {
			log.Warnf("Layer3 CUDN %s never reached NetworkAllocationSucceeded=True, NetworkAllocationLatency will be -1", m.Name)
		}
		c.NormLatencies = append(c.NormLatencies, m)
		return true
	})
	return 0
}

func (c *cudnLatency) getLatency(normLatency any) map[string]float64 {
	m := normLatency.(cudnMetric)
	latencies := map[string]float64{
		"NetworkCreatedLatency": float64(m.NetworkCreatedLatency),
	}
	// Layer3-only metric; -1 means not recorded (Layer2 or condition never
	// reached), so it must not enter the quantiles.
	if m.NetworkAllocationLatency >= 0 {
		latencies["NetworkAllocationLatency"] = float64(m.NetworkAllocationLatency)
	}
	return latencies
}

func (c *cudnLatency) IsCompatible() bool {
	return slices.Contains(supportedCudnLatencyJobTypes, c.JobConfig.JobType)
}
