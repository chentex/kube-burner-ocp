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
	"strings"
	"time"

	kubeburnermeasurements "github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/workloads"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/kube-burner/kube-burner-ocp/pkg/measurements"
)

var udnCncMeasurementFactoryMap = map[string]kubeburnermeasurements.NewMeasurementFactory{
	"cudnLatency":      measurements.NewCudnLatencyMeasurementFactory,
	"cncLatency":       measurements.NewCncLatencyMeasurementFactory,
	"dataplaneLatency": measurements.NewDataplaneLatencyMeasurementFactory,
}

// NewUDNCNC holds udn-cnc workload
//
// Every mode creates M = --iterations independent groups of S = --cudns CUDNs
// (S defaults to iterations when not set). Each group spans S namespaces and
// receives a fixed number of ClusterNetworkConnects depending on the mode:
//   - pair:     S/2 CNCs, one per (1,2), (3,4), ... pair of CUDNs in the group
//   - mesh:     1 CNC connecting all S CUDNs of the group
//   - multiply: S*(S-1)/2 CNCs, full mesh over the group's CUDNs
func NewUDNCNC(wh *workloads.WorkloadHelper) *cobra.Command {
	var iterations, cudns, parallelStreams, duration int
	var pprof, reverse bool
	var connectivity, topology, mode, protocol string
	var podReadyThreshold, pprofInterval, jobPause, cncReadyTimeout time.Duration
	var metricsProfiles []string
	var rc int
	cmd := &cobra.Command{
		Use:          "udn-cnc",
		Short:        "Runs udn-cnc workload testing ClusterNetworkConnect between CUDN groups",
		SilenceUsage: true,
		Run: func(cmd *cobra.Command, args []string) {
			if iterations < 1 {
				log.Fatalf("--iterations must be >= 1")
			}
			switch mode {
			case "pair", "mesh", "multiply":
			default:
				log.Fatalf("invalid mode %q: must be pair, mesh, or multiply", mode)
			}
			if !strings.Contains(connectivity, "ServiceNetwork") {
				log.Fatalf("--connectivity must include ServiceNetwork: all protocols connect via ClusterIP services")
			}
			meshSize := cudns
			if meshSize == 0 {
				meshSize = iterations
			}
			if meshSize < 1 {
				log.Fatalf("--cudns must be >= 1")
			}
			var cncsPerGroup int
			switch mode {
			case "pair":
				if meshSize < 2 || meshSize%2 != 0 {
					log.Fatalf("pair mode requires an even --cudns >= 2")
				}
				cncsPerGroup = meshSize / 2
			case "mesh":
				if meshSize < 2 {
					log.Fatalf("mesh mode requires --cudns >= 2 (got %d)", meshSize)
				}
				cncsPerGroup = 1
			case "multiply":
				if meshSize < 2 {
					log.Fatalf("multiply mode requires --cudns >= 2 (got %d)", meshSize)
				}
				cncsPerGroup = meshSize * (meshSize - 1) / 2
			}
			numNamespaces := iterations * meshSize
			numCNCs := iterations * cncsPerGroup
			// One entry per CNC: [group, localIdxA, localIdxB]
			pairs := make([][]int, 0, numCNCs)
			for m := 0; m < iterations; m++ {
				if mode == "pair" {
					for k := 0; k < cncsPerGroup; k++ {
						pairs = append(pairs, []int{m, 2 * k, 2*k + 1})
					}
				} else {
					for i := 0; i < meshSize; i++ {
						for j := i + 1; j < meshSize; j++ {
							pairs = append(pairs, []int{m, i, j})
						}
					}
				}
			}
			pairsJSON, err := json.Marshal(pairs)
			if err != nil {
				log.Fatalf("failed to marshal pairs: %v", err)
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
			AdditionalVars["MESH_SIZE"] = meshSize
			AdditionalVars["PAIRS_JSON"] = string(pairsJSON)
			AdditionalVars["POD_READY_THRESHOLD"] = podReadyThreshold
			measurements.UDNCNCMode = mode
			measurements.UDNCNCIterations = iterations
			measurements.UDNCNCProtocol = protocol
			measurements.UDNCNCParallelStreams = parallelStreams
			measurements.UDNCNCDuration = duration
			measurements.UDNCNCReverse = reverse
			measurements.UDNCNCMeshSize = meshSize
			wh.SetMeasurements(udnCncMeasurementFactoryMap)
			// Gate: do not create any objects until the ClusterNetworkConnect CRD
			// is established (the udn-cnc workload cannot run without it).
			if err := waitClusterNetworkConnectCRD(cmd); err != nil {
				log.Fatalf("ERROR: %v — the udn-cnc workload cannot run", err)
			}
			rc = RunWorkload(cmd, wh, cmd.Name()+".yml")
		},
		PostRun: func(cmd *cobra.Command, args []string) {
			os.Exit(rc)
		},
	}
	cmd.Flags().IntVar(&iterations, "iterations", 0, "Number of iterations (independent CUDN groups)")
	cmd.Flags().StringVar(&connectivity, "connectivity", "PodNetwork,ServiceNetwork", "Comma-separated connectivity types for ClusterNetworkConnect")
	cmd.Flags().StringVar(&topology, "topology", "Layer2", "CUDN topology: Layer2 or Layer3")
	cmd.Flags().StringVar(&mode, "mode", "pair", "Connectivity mode: pair (S/2 CNCs per iteration), mesh (1 CNC per iteration), multiply (full mesh, S*(S-1)/2 CNCs per iteration); S = --cudns")
	cmd.Flags().StringVar(&protocol, "protocol", "tcp", "dataplane measurement protocol: tcp, udp (iperf3), or tcp_rr (netperf request/response latency)")
	cmd.Flags().IntVar(&parallelStreams, "parallel", 1, "Number of parallel iperf3 streams per test (-P)")
	cmd.Flags().IntVar(&duration, "duration", 10, "iperf3 test duration in seconds per packet size (-t)")
	cmd.Flags().BoolVar(&reverse, "reverse", false, "Run iperf3 in reverse mode: server→client traffic (-R, TCP only)")
	cmd.Flags().IntVar(&cudns, "cudns", 0, "Number of CUDNs created per iteration in every mode (default: same as --iterations)")
	cmd.Flags().DurationVar(&podReadyThreshold, "pod-ready-threshold", 2*time.Minute, "Pod ready timeout threshold")
	cmd.Flags().DurationVar(&jobPause, "job-pause", 1*time.Minute, "Pause after CNC creation to allow OVN-K network settling")
	cmd.Flags().DurationVar(&cncReadyTimeout, "cnc-ready-timeout", 20*time.Minute, "Timeout to wait for the ClusterNetworkConnect CRD to be established before starting the workload")
	cmd.Flags().BoolVar(&pprof, "pprof", false, "Enable pprof collection for ovnkube components")
	cmd.Flags().DurationVar(&pprofInterval, "pprof-interval", 0, "Interval between pprof collections")
	cmd.Flags().StringSliceVar(&metricsProfiles, "metrics-profile", []string{"metrics.yml"}, "Comma separated list of metrics profiles to use")
	cmd.MarkFlagRequired("iterations")
	return cmd
}
