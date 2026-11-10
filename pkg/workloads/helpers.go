// Copyright 2023 The Kube-burner Authors.
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
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	k8sconnector "github.com/cloud-bulldozer/go-commons/v2/k8s-connector"
	k8sstorage "github.com/cloud-bulldozer/go-commons/v2/k8s-storage"
	ocpmetadata "github.com/cloud-bulldozer/go-commons/v2/ocp-metadata"
	"github.com/kube-burner/kube-burner/v2/pkg/config"
	kubeburnerutil "github.com/kube-burner/kube-burner/v2/pkg/util"
	"github.com/kube-burner/kube-burner/v2/pkg/workloads"
	configv1 "github.com/openshift/api/config/v1"
	"github.com/openshift/client-go/config/clientset/versioned"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	v1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
)

const (
	kubeBurnerTestNameLabelKey = "kube-burner.io/test-name"
)

var (
	clusterMetadata      ocpmetadata.ClusterMetadata
	AdditionalVars       map[string]any
	SetVars              map[string]any
	accessModeTranslator = map[string]string{
		"RO":  "ReadOnly",
		"RWO": "ReadWriteOnce",
		"RWX": "ReadWriteMany",
	}
)

func setMetrics(cmd *cobra.Command, metricsProfiles []string) {
	profileType, _ := cmd.Root().PersistentFlags().GetString("profile-type")
	switch ProfileType(profileType) {
	case Reporting:
		metricsProfiles = []string{"metrics-report.yml"}
	case Both:
		metricsProfiles = append(metricsProfiles, "metrics-report.yml")
	}
	os.Setenv("METRICS", strings.Join(metricsProfiles, ","))
}

const networkConnectFeatureGate configv1.FeatureGateName = "NetworkConnect"

// waitClusterNetworkConnectCRD enables the NetworkConnect feature gate (which
// the udn-cnc workload requires and which is only present on beta/dev-preview
// clusters) and then blocks until the network operator has rolled out and
// established the ClusterNetworkConnect CRD. The udn-cnc workload cannot create
// any ClusterNetworkConnect objects until the CRD exists. It returns an error
// when the feature gate cannot be enabled or the timeout is reached.
func waitClusterNetworkConnectCRD(cmd *cobra.Command) error {
	timeout := 20 * time.Minute
	if v, err := cmd.Flags().GetDuration("cnc-ready-timeout"); err == nil {
		timeout = v
	}
	_, restConfig := config.NewKubeClientProvider("", "").ClientSet(0, 0)
	openshiftClientset, err := versioned.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("error creating OpenShift clientset: %w", err)
	}
	if err := enableNetworkConnectFeatureGate(openshiftClientset); err != nil {
		return err
	}
	return waitForCRDEstablished(dynamic.NewForConfigOrDie(restConfig), "clusternetworkconnects.k8s.ovn.org", 5*time.Second, timeout)
}

// enableNetworkConnectFeatureGate ensures the NetworkConnect feature gate is
// enabled. It is idempotent: when the gate is already present in
// customNoUpgrade.enabled it makes no change. Otherwise it switches the
// featureSet to CustomNoUpgrade (which cannot be changed back) and appends the
// gate, retrying on conflict. This is required because the workload only runs
// on beta clusters where the gate is not enabled by default.
func enableNetworkConnectFeatureGate(clientset versioned.Interface) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fg, err := clientset.ConfigV1().FeatureGates().Get(context.Background(), "cluster", metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting featuregate cluster: %w", err)
		}
		custom := fg.Spec.CustomNoUpgrade
		if custom == nil {
			custom = &configv1.CustomFeatureGates{}
		}
		// Idempotency: leave the gate untouched when already enabled.
		for _, name := range custom.Enabled {
			if name == networkConnectFeatureGate {
				log.Infof("Feature gate %q is already enabled", networkConnectFeatureGate)
				return nil
			}
		}
		log.Infof("Enabling feature gate %q (featureSet=CustomNoUpgrade, prevents upgrades)", networkConnectFeatureGate)
		fg.Spec.FeatureSet = configv1.CustomNoUpgrade
		custom.Enabled = append(custom.Enabled, networkConnectFeatureGate)
		fg.Spec.CustomNoUpgrade = custom
		if _, err := clientset.ConfigV1().FeatureGates().Update(context.Background(), fg, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("error updating featuregate cluster: %w", err)
		}
		log.Infof("Feature gate %q enabled", networkConnectFeatureGate)
		return nil
	})
}

// waitForCRDEstablished polls the given CRD until its Established condition is
// True. It returns an error when the CRD is not established within the timeout
// or when the API call fails with a non-NotFound error.
func waitForCRDEstablished(dyn dynamic.Interface, crdName string, pollInterval, timeout time.Duration) error {
	crdGVR := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	log.Infof("Waiting for %s CRD to be established (timeout %s)...", crdName, timeout)
	err := wait.PollUntilContextTimeout(context.Background(), pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		crd, err := dyn.Resource(crdGVR).Get(ctx, crdName, metav1.GetOptions{})
		if err != nil {
			if kerrors.IsNotFound(err) {
				log.Debugf("%s CRD not found yet", crdName)
				return false, nil
			}
			return false, err
		}
		conds, _, _ := unstructured.NestedSlice(crd.UnstructuredContent(), "status", "conditions")
		for _, c := range conds {
			cond, ok := c.(map[string]any)
			if !ok {
				continue
			}
			if cond["type"] == "Established" && cond["status"] == "True" {
				log.Infof("%s CRD is established", crdName)
				return true, nil
			}
		}
		log.Debugf("%s CRD not established yet", crdName)
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("%s CRD not established within %s: %w", crdName, timeout, err)
	}
	return nil
}

// SetKubeBurnerFlags configures the required environment variables and flags for kube-burner
func GatherMetadata(wh *workloads.WorkloadHelper) error {
	var err error
	kubeClientProvider := config.NewKubeClientProvider("", "")
	_, restConfig := kubeClientProvider.DefaultClientSet()
	wh.MetadataAgent, err = ocpmetadata.NewMetadata(restConfig)
	if err != nil {
		return err
	}
	clusterMetadata, err = wh.MetadataAgent.GetClusterMetadata()
	if err != nil {
		return err
	}
	jsonData, err := json.Marshal(clusterMetadata)
	if err != nil {
		return err
	}
	json.Unmarshal(jsonData, &wh.SummaryMetadata)
	wh.MetricsMetadata = map[string]any{
		"ocpMajorVersion": clusterMetadata.OCPMajorVersion,
		"ocpVersion":      clusterMetadata.OCPVersion,
	}
	return nil
}

func getK8SConnector() k8sconnector.K8SConnector {
	kubeClientProvider := config.NewKubeClientProvider("", "")
	_, restConfig := kubeClientProvider.DefaultClientSet()
	k8sConnector, err := k8sconnector.NewK8SConnector(restConfig)
	if err != nil {
		log.Fatal(err)
	}
	return k8sConnector
}

func generateLoopCounterSlice(length, startValue int) []string {
	counter := make([]string, length)
	for i := range length {
		counter[i] = fmt.Sprint(i + startValue)
	}
	return counter
}

// addWorkloadFlagsToMetadata adds all flag values from the command to SummaryMetadata
func addWorkloadFlagsToMetadata(cmd *cobra.Command, wh *workloads.WorkloadHelper) {
	workloadFlags := make(map[string]string)
	// Use LocalFlags() instead of Flags() to only get flags specific to this command
	cmd.LocalFlags().VisitAll(func(flag *pflag.Flag) {
		if flag.Name == "help" {
			return
		}
		flagName := kebabToCamelCase(flag.Name)
		workloadFlags[flagName] = flag.Value.String()
	})
	wh.SummaryMetadata["workloadFlags"] = workloadFlags
}

// RunWorkload executes the common workload pattern: adds flags to metadata, sets variables, and runs the workload
func RunWorkload(cmd *cobra.Command, wh *workloads.WorkloadHelper, configFile string) int {
	addWorkloadFlagsToMetadata(cmd, wh)
	wh.SetVariables(AdditionalVars, SetVars)
	return wh.Run(configFile)
}

// kebabToCamelCase converts a flag name from kebab-case to camelCase
func kebabToCamelCase(word string) string {
	parts := strings.Split(word, "-")
	for i := range parts {
		// First part stays lowercase, rest are capitalized
		if i == 0 {
			continue
		}
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}

// Add metadata specific to the CNV workloads
func AddVirtMetadata(wh *workloads.WorkloadHelper, vmImage, udnLayer, udnBindingMethod string) error {
	var err error
	var cnvVersion string
	kubeClientProvider := config.NewKubeClientProvider("", "")
	_, restConfig := kubeClientProvider.DefaultClientSet()
	wh.MetadataAgent, err = ocpmetadata.NewMetadata(restConfig)
	if err != nil {
		return err
	}
	cnvVersion, err = wh.MetadataAgent.GetOCPVirtualizationVersion()
	if err != nil {
		return err
	}
	wh.SummaryMetadata["OCPVirtualizationVersion"] = cnvVersion
	if udnLayer != "" {
		wh.SummaryMetadata["UdnLayer"] = udnLayer
		wh.SummaryMetadata["UdnBindingMethod"] = udnBindingMethod
	}
	wh.SummaryMetadata["VmImage"] = vmImage
	return nil
}

func getStorageAndSnapshotClasses(storageClassNameParam string, useSnapshot, useSnapshotChanged bool) (string, string) {
	k8sConnector := getK8SConnector()

	// Verify provided storage class name or get default of cluster
	storageClassName, err := k8sstorage.GetStorageClassName(k8sConnector, storageClassNameParam, true)
	if err != nil {
		log.Fatal(err)
	}
	if storageClassName == "" {
		if storageClassNameParam == "" {
			log.Fatal("No default StorageClass is set and another was not provided")
		} else {
			log.Fatalf("Provided StorageClass [%v] does not exist", storageClassNameParam)
		}
	}
	log.Infof("Running tests with Storage Class [%s]", storageClassName)

	// If user did not set use-snapshot, get the value from the StorageProfile
	if !useSnapshotChanged {
		sourceFormat, err := k8sstorage.GetDataImportCronSourceFormatForStorageClass(k8sConnector, storageClassName)
		if err != nil {
			log.Fatalf("Failed to get source format for StorageClass [%s] - %v", storageClassName, err)
		}
		useSnapshot = sourceFormat == "snapshot"
		log.Info("The flag use-snapshot was not set. Using the value from the StorageProfile: ", useSnapshot)
	}

	var volumeSnapshotClassName string
	// If using Snapshot, get the VolumeSnapshotClass with the same provisioner as the StorageClass
	if useSnapshot {
		volumeSnapshotClassName, err = k8sstorage.GetVolumeSnapshotClassNameForStorageClass(k8sConnector, storageClassName)
		if err != nil {
			log.Fatalf("Failed to get VolumeSnapshotClass for StorageClass %s - %v", storageClassName, err)
		}
		if volumeSnapshotClassName == "" {
			log.Fatalf("Could not find a corresponding VolumeSnapshotClass for StorageClass %s", storageClassName)
		}
		log.Infof("Running tests with VolumeSnapshotClass [%s]", volumeSnapshotClassName)
	}

	return storageClassName, volumeSnapshotClassName
}

func deletePVsForNamespaces(ctx context.Context, connector k8sconnector.K8SConnector, namespaceNamesMap map[string]struct{}) {
	pvs, err := connector.ClientSet().CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Warnf("Failed listing PVs - %s", err)
		return
	}
	deletingPVs := make(map[string]struct{})
	for _, pv := range pvs.Items {
		// PV not claimed
		if pv.Spec.ClaimRef == nil {
			continue
		}
		// PV not claimed by test namespace
		if _, ok := namespaceNamesMap[pv.Spec.ClaimRef.Namespace]; !ok {
			continue
		}
		// PV will be deleted automatically
		if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimDelete {
			err = connector.ClientSet().CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{})
			if err != nil {
				log.Warnf("Failed to delete PV [%s]: %v", pv.Name, err)
				continue
			}
		}
		deletingPVs[pv.Name] = struct{}{}
	}

	err = wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		pvs, err := connector.ClientSet().CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		for _, pv := range pvs.Items {
			if _, ok := deletingPVs[pv.Name]; ok {
				log.Debugf("Waiting for PV [%s] to be deleted", pv.Name)
				return false, nil
			}
		}
		log.Info("All deleted PVs are deleted")
		return true, nil
	})
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Fatalf("Timeout cleaning up PersistentVolumes: %v", err)
		}
		log.Errorf("Error cleaning up PersistentVolumes: %v", err)
	}
}

func deleteVolumeSnapshotContentForNamespaces(ctx context.Context, connector k8sconnector.K8SConnector, namespaceNamesMap map[string]struct{}) {
	volumeSnapshotContentGVR := schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents"}
	itemList, err := connector.DynamicClient().Resource(volumeSnapshotContentGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Warnf("Failed listing VolumeSnapshotContents - %s", err)
		return
	}
	deletingVSCs := make(map[string]struct{})
	for _, vsc := range itemList.Items {
		namespace, found, err := unstructured.NestedString(vsc.Object, "spec", "volumeSnapshotRef", "namespace")
		if err != nil {
			log.Warnf("Error reading namespace of volumeSnapshotRef from VolumeSnapshotContent %s: %v", vsc.GetName(), err)
			continue
		}
		if !found {
			log.Warnf("Namespace not found in volumeSnapshotRef of VolumeSnapshotContent %s", vsc.GetName())
			continue
		}
		// VolumeSnapshotContent does not belong to the test namespace
		if _, ok := namespaceNamesMap[namespace]; !ok {
			continue
		}
		deletionPolicy, found, err := unstructured.NestedString(vsc.Object, "spec", "deletionPolicy")
		if err != nil {
			log.Warnf("Error reading deletionPolicy from VolumeSnapshotContent %s: %v", vsc.GetName(), err)
			continue
		}
		if !found {
			log.Warnf("deletionPolicy not found in VolumeSnapshotContent %s", vsc.GetName())
			continue
		}
		// VolumeSnapshotContent will be deleted automatically
		if deletionPolicy != "Delete" {
			err = connector.DynamicClient().Resource(volumeSnapshotContentGVR).Delete(ctx, vsc.GetName(), metav1.DeleteOptions{})
			if err != nil {
				log.Warnf("Failed to delete VolumeSnapshotContent [%s]: %v", vsc.GetName(), err)
				continue
			}
		}
		deletingVSCs[vsc.GetName()] = struct{}{}
	}

	err = wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		itemList, err := connector.DynamicClient().Resource(volumeSnapshotContentGVR).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		for _, vsc := range itemList.Items {
			if _, ok := deletingVSCs[vsc.GetName()]; ok {
				log.Debugf("Waiting for VolumeSnapshotContent [%s] to be deleted", vsc.GetName())
				return false, nil
			}
		}
		log.Info("All deleted VolumeSnapshotContent are deleted")
		return true, nil
	})
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Fatalf("Timeout cleaning up VolumeSnapshotContent: %v", err)
		}
		log.Errorf("Error cleaning up VolumeSnapshotContent: %v", err)
	}
}

func cleanupTestNamespaces(ctx context.Context, labelSelector string) {
	k8sConnector := getK8SConnector()
	ns, err := k8sConnector.ClientSet().CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		log.Fatalf("Error listing namespaces: %v", err.Error())
	}

	if len(ns.Items) == 0 {
		log.Info("No Namespaces were found for previous test")
		return
	}

	kubeburnerutil.CleanupNamespacesByLabel(ctx, k8sConnector.ClientSet(), labelSelector)

	namespaceNamesMap := make(map[string]struct{}, len(ns.Items))
	for _, ns := range ns.Items {
		namespaceNamesMap[ns.Name] = struct{}{}
	}

	deleteVolumeSnapshotContentForNamespaces(ctx, k8sConnector, namespaceNamesMap)
	deletePVsForNamespaces(ctx, k8sConnector, namespaceNamesMap)

}

func verifyOrGetRandomWorkerNodeName(workerNodeName string) string {
	k8sConnector := getK8SConnector()

	nodes, err := k8sConnector.ClientSet().CoreV1().Nodes().List(context.Background(), metav1.ListOptions{LabelSelector: "node-role.kubernetes.io/worker"})
	if err != nil {
		log.Fatalf("Error getting nodes: %v", err)
		return ""
	}

	workerNodeNamesMap := make(map[string]struct{}, len(nodes.Items))
	for _, node := range nodes.Items {
		workerNodeNamesMap[node.Name] = struct{}{}
	}

	if workerNodeName != "" {
		if _, ok := workerNodeNamesMap[workerNodeName]; !ok {
			log.Fatalf("Provided worker node %s does not exist", workerNodeName)
		}
		return workerNodeName
	}

	workerNodeNamesArray := make([]string, 0, len(workerNodeNamesMap))
	for k := range workerNodeNamesMap {
		workerNodeNamesArray = append(workerNodeNamesArray, k)
	}

	return workerNodeNamesArray[rand.Intn(len(workerNodeNamesArray))]
}
