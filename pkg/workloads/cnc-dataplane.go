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
	"os"
	"time"

	kubeburnermeasurements "github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/workloads"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/kube-burner/kube-burner-ocp/pkg/measurements"
)

var cncDataplaneMeasurementFactoryMap = map[string]kubeburnermeasurements.NewMeasurementFactory{
	"dataplaneLatency": measurements.NewDataplaneLatencyMeasurementFactory,
}

const (
	cncDataplaneNsPrefix = "cnc-dataplane"
	cncDataplaneSvcName  = "cnc-svc"
)

// NewCNCDataplane holds the cnc-dataplane workload
//
// Same data plane tests as udn-cnc (iperf3 tcp/udp, netperf tcp_rr between
// client and server pods) but over the default cluster network: regular
// namespaces, Deployments and ClusterIP Services, no UDNs/CUDNs/CNCs.
//
// Every mode creates M = --iterations independent groups of S = --services
// namespaces (S defaults to iterations when not set). Each namespace holds a
// server Deployment with a ClusterIP service and a client Deployment
// connecting to a peer's service. Connectivity pairs depend on the mode:
//   - pair:     S/2 pairs, one per (1,2), (3,4), ... pair of namespaces in the group
//   - mesh:     all intra-group pairs, clients connect to all S-1 other services
//   - multiply: same as mesh; the mode labels the run as a full-mesh data plane test
func NewCNCDataplane(wh *workloads.WorkloadHelper) *cobra.Command {
	var iterations, services, parallelStreams, duration int
	var pprof, reverse bool
	var mode, protocol string
	var podReadyThreshold, pprofInterval, jobPause time.Duration
	var metricsProfiles []string
	var rc int
	cmd := &cobra.Command{
		Use:          "cnc-dataplane",
		Short:        "Runs data plane tests (iperf3/netperf client-server pairs) over regular services and deployments",
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
			meshSize := services
			if meshSize == 0 {
				meshSize = iterations
			}
			if meshSize < 1 {
				log.Fatalf("--services must be >= 1")
			}
			if mode == "pair" {
				if meshSize < 2 || meshSize%2 != 0 {
					log.Fatalf("pair mode requires an even --services >= 2")
				}
			} else if meshSize < 2 {
				log.Fatalf("%s mode requires --services >= 2 (got %d)", mode, meshSize)
			}
			numNamespaces := iterations * meshSize
			setMetrics(cmd, metricsProfiles)
			AdditionalVars["PPROF"] = pprof
			AdditionalVars["PPROF_INTERVAL"] = pprofInterval.String()
			AdditionalVars["JOB_PAUSE"] = jobPause
			AdditionalVars["JOB_ITERATIONS"] = iterations
			AdditionalVars["NUM_NAMESPACES"] = numNamespaces
			AdditionalVars["MODE"] = mode
			AdditionalVars["MESH_SIZE"] = meshSize
			AdditionalVars["NS_PREFIX"] = cncDataplaneNsPrefix
			AdditionalVars["POD_READY_THRESHOLD"] = podReadyThreshold
			// Configure the shared dataplaneLatency measurement for this workload:
			// namespace prefix and the service the clients connect to.
			measurements.UDNCNCMode = mode
			measurements.UDNCNCIterations = iterations
			measurements.UDNCNCRuntimeNsPrefix = cncDataplaneNsPrefix
			measurements.UDNCNCRuntimeSvcName = cncDataplaneSvcName
			measurements.UDNCNCProtocol = protocol
			measurements.UDNCNCParallelStreams = parallelStreams
			measurements.UDNCNCDuration = duration
			measurements.UDNCNCReverse = reverse
			measurements.UDNCNCMeshSize = meshSize
			wh.SetMeasurements(cncDataplaneMeasurementFactoryMap)
			rc = RunWorkload(cmd, wh, cmd.Name()+".yml")
		},
		PostRun: func(cmd *cobra.Command, args []string) {
			os.Exit(rc)
		},
	}
	cmd.Flags().IntVar(&iterations, "iterations", 0, "Number of iterations (independent service groups)")
	cmd.Flags().StringVar(&mode, "mode", "pair", "Connectivity mode: pair (S/2 client-server pairs per iteration), mesh or multiply (each client connects to all S-1 peers); S = --services")
	cmd.Flags().StringVar(&protocol, "protocol", "tcp", "dataplane measurement protocol: tcp, udp (iperf3), or tcp_rr (netperf request/response latency)")
	cmd.Flags().IntVar(&parallelStreams, "parallel", 1, "Number of parallel iperf3 streams per test (-P)")
	cmd.Flags().IntVar(&duration, "duration", 10, "iperf3 test duration in seconds per packet size (-t)")
	cmd.Flags().BoolVar(&reverse, "reverse", false, "Run iperf3 in reverse mode: server→client traffic (-R, TCP only)")
	cmd.Flags().IntVar(&services, "services", 0, "Number of client-server services created per iteration in every mode (default: same as --iterations)")
	cmd.Flags().DurationVar(&podReadyThreshold, "pod-ready-threshold", 2*time.Minute, "Pod ready timeout threshold")
	cmd.Flags().DurationVar(&jobPause, "job-pause", 1*time.Minute, "Pause before the dataplane test to allow networking to settle")
	cmd.Flags().BoolVar(&pprof, "pprof", false, "Enable pprof collection for ovnkube components")
	cmd.Flags().DurationVar(&pprofInterval, "pprof-interval", 0, "Interval between pprof collections")
	cmd.Flags().StringSliceVar(&metricsProfiles, "metrics-profile", []string{"metrics.yml"}, "Comma separated list of metrics profiles to use")
	cmd.MarkFlagRequired("iterations")
	return cmd
}
