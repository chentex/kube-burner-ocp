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

package workloads

import (
	"encoding/json"
	"os"
	"time"

	kubeburnermeasurements "github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/workloads"
	"github.com/spf13/cobra"

	"github.com/kube-burner/kube-burner-ocp/pkg/measurements"
)

var udnCncMeasurementFactoryMap = map[string]kubeburnermeasurements.NewMeasurementFactory{
	"cudnLatency":      measurements.NewCudnLatencyMeasurementFactory,
	"cncLatency":       measurements.NewCncLatencyMeasurementFactory,
	"dataplaneLatency": measurements.NewDataplaneLatencyMeasurementFactory,
}

// NewUDNCNC holds udn-cnc workload
func NewUDNCNC(wh *workloads.WorkloadHelper) *cobra.Command {
	var iterations, cudns, parallelStreams, duration int
	var pprof, reverse bool
	var connectivity, topology, mode, protocol string
	var podReadyThreshold, pprofInterval, jobPause time.Duration
	var metricsProfiles []string
	var rc int
	cmd := &cobra.Command{
		Use:          "udn-cnc",
		Short:        "Runs udn-cnc workload testing ClusterNetworkConnect between CUDN pairs",
		SilenceUsage: true,
		Run: func(cmd *cobra.Command, args []string) {
			numNamespaces := iterations * 2
			numCNCs := iterations
			configFile := cmd.Name() + ".yml"
			if mode == "mesh" {
				numNamespaces = iterations
				numCNCs = 1
			} else if mode == "multiply" {
				if cudns == 0 {
					// legacy: treat iterations as mesh size, 1 mesh
					cudns = iterations
					iterations = 1
				}
				meshSize := cudns
				numNamespaces = iterations * meshSize
				numCNCs = iterations * meshSize * (meshSize - 1) / 2
				pairs := make([][]int, 0, numCNCs)
				for m := 0; m < iterations; m++ {
					for i := 0; i < meshSize; i++ {
						for j := i + 1; j < meshSize; j++ {
							pairs = append(pairs, []int{m, i, j})
						}
					}
				}
				pairsJSON, _ := json.Marshal(pairs)
				AdditionalVars["PAIRS_JSON"] = string(pairsJSON)
				AdditionalVars["MESH_SIZE"] = meshSize
				measurements.UDNCNCMeshSize = meshSize
				configFile = cmd.Name() + "-multiply.yml"
			}
			setMetrics(cmd, metricsProfiles)
			AdditionalVars["PPROF"] = pprof
			AdditionalVars["PPROF_INTERVAL"] = pprofInterval.String()
			AdditionalVars["JOB_PAUSE"] = jobPause
			AdditionalVars["JOB_ITERATIONS"] = iterations
			AdditionalVars["NUM_NAMESPACES"] = numNamespaces
			AdditionalVars["NUM_CNCS"] = numCNCs
			AdditionalVars["CONNECTIVITY"] = connectivity
			AdditionalVars["TOPOLOGY"] = topology
			AdditionalVars["MODE"] = mode
			AdditionalVars["POD_READY_THRESHOLD"] = podReadyThreshold
			measurements.UDNCNCMode = mode
			measurements.UDNCNCProtocol = protocol
			measurements.UDNCNCParallelStreams = parallelStreams
			measurements.UDNCNCDuration = duration
			measurements.UDNCNCReverse = reverse
			wh.SetMeasurements(udnCncMeasurementFactoryMap)
			rc = RunWorkload(cmd, wh, configFile)
		},
		PostRun: func(cmd *cobra.Command, args []string) {
			os.Exit(rc)
		},
	}
	cmd.Flags().IntVar(&iterations, "iterations", 0, "Number of iterations: pairs in pair mode, namespaces in mesh mode")
	cmd.Flags().StringVar(&connectivity, "connectivity", "PodNetwork,ServiceNetwork", "Comma-separated connectivity types for ClusterNetworkConnect")
	cmd.Flags().StringVar(&topology, "topology", "Layer2", "CUDN topology: Layer2 or Layer3")
	cmd.Flags().StringVar(&mode, "mode", "pair", "Connectivity mode: pair (N pairs, N CNCs), mesh (N namespaces, 1 CNC connecting all), or multiply (N namespaces, N*(N-1)/2 CNCs for full mesh)")
	cmd.Flags().StringVar(&protocol, "protocol", "tcp", "dataplane measurement protocol: tcp, udp (iperf3), or tcp_rr (netperf request/response latency)")
	cmd.Flags().IntVar(&parallelStreams, "parallel", 1, "Number of parallel iperf3 streams per test (-P)")
	cmd.Flags().IntVar(&duration, "duration", 10, "iperf3 test duration in seconds per packet size (-t)")
	cmd.Flags().BoolVar(&reverse, "reverse", false, "Run iperf3 in reverse mode: server→client traffic (-R, TCP only)")
	cmd.Flags().IntVar(&cudns, "cudns", 0, "multiply mode only: total CUDNs forming the mesh (default: same as --iterations); --iterations then controls how many of those namespaces receive workload pods")
	cmd.Flags().DurationVar(&podReadyThreshold, "pod-ready-threshold", 2*time.Minute, "Pod ready timeout threshold")
	cmd.Flags().DurationVar(&jobPause, "job-pause", 1*time.Minute, "Pause after CNC creation to allow OVN-K network settling")
	cmd.Flags().BoolVar(&pprof, "pprof", false, "Enable pprof collection for ovnkube components")
	cmd.Flags().DurationVar(&pprofInterval, "pprof-interval", 0, "Interval between pprof collections")
	cmd.Flags().StringSliceVar(&metricsProfiles, "metrics-profile", []string{"metrics.yml"}, "Comma separated list of metrics profiles to use")
	cmd.MarkFlagRequired("iterations")
	return cmd
}
