// Copyright 2026 The Kube-burner Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed under the License.
// See the License for the specific language governing permissions and
// limitations under the License.

package workloads

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	openshiftfake "github.com/openshift/client-go/config/clientset/versioned/fake"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	crdTestName     = "clusternetworkconnects.k8s.ovn.org"
	crdTestListKind = "CustomResourceDefinitionList"
)

var crdTestGVR = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}

func newFakeFeatureGateClient(t *testing.T, fg *configv1.FeatureGate) *openshiftfake.Clientset {
	t.Helper()
	var objs []runtime.Object
	if fg != nil {
		objs = append(objs, fg)
	}
	return openshiftfake.NewSimpleClientset(objs...)
}

func TestEnableNetworkConnectFeatureGate(t *testing.T) {
	t.Run("already enabled: no update", func(t *testing.T) {
		fg := &configv1.FeatureGate{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
			Spec: configv1.FeatureGateSpec{
				FeatureGateSelection: configv1.FeatureGateSelection{
					FeatureSet: configv1.CustomNoUpgrade,
					CustomNoUpgrade: &configv1.CustomFeatureGates{
						Enabled: []configv1.FeatureGateName{"OtherGate", networkConnectFeatureGate},
					},
				},
			},
		}
		client := newFakeFeatureGateClient(t, fg)
		if err := enableNetworkConnectFeatureGate(client); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if n := len(client.Actions()); n != 1 {
			t.Fatalf("expected exactly 1 action (get), got %d: %v", n, client.Actions())
		}
		got, err := client.ConfigV1().FeatureGates().Get(context.Background(), "cluster", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Spec.CustomNoUpgrade.Enabled, []configv1.FeatureGateName{"OtherGate", networkConnectFeatureGate}) {
			t.Fatalf("spec changed unexpectedly: %v", got.Spec.CustomNoUpgrade.Enabled)
		}
	})

	t.Run("disabled: sets CustomNoUpgrade and appends gate", func(t *testing.T) {
		fg := &configv1.FeatureGate{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
			Spec: configv1.FeatureGateSpec{
				FeatureGateSelection: configv1.FeatureGateSelection{
					FeatureSet: configv1.Default,
				},
			},
		}
		client := newFakeFeatureGateClient(t, fg)
		if err := enableNetworkConnectFeatureGate(client); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		got, err := client.ConfigV1().FeatureGates().Get(context.Background(), "cluster", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got.Spec.FeatureSet != configv1.CustomNoUpgrade {
			t.Fatalf("expected featureSet CustomNoUpgrade, got %q", got.Spec.FeatureSet)
		}
		if len(got.Spec.CustomNoUpgrade.Enabled) != 1 || got.Spec.CustomNoUpgrade.Enabled[0] != networkConnectFeatureGate {
			t.Fatalf("expected [NetworkConnect], got %v", got.Spec.CustomNoUpgrade.Enabled)
		}
	})

	t.Run("other gate already set: preserves it and appends", func(t *testing.T) {
		fg := &configv1.FeatureGate{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
			Spec: configv1.FeatureGateSpec{
				FeatureGateSelection: configv1.FeatureGateSelection{
					FeatureSet: configv1.CustomNoUpgrade,
					CustomNoUpgrade: &configv1.CustomFeatureGates{
						Enabled: []configv1.FeatureGateName{"SomeOtherGate"},
					},
				},
			},
		}
		client := newFakeFeatureGateClient(t, fg)
		if err := enableNetworkConnectFeatureGate(client); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		got, err := client.ConfigV1().FeatureGates().Get(context.Background(), "cluster", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		want := []configv1.FeatureGateName{"SomeOtherGate", networkConnectFeatureGate}
		if !reflect.DeepEqual(got.Spec.CustomNoUpgrade.Enabled, want) {
			t.Fatalf("expected %v, got %v", want, got.Spec.CustomNoUpgrade.Enabled)
		}
	})

	t.Run("featuregate not found: error", func(t *testing.T) {
		client := newFakeFeatureGateClient(t, nil)
		if err := enableNetworkConnectFeatureGate(client); err == nil {
			t.Fatal("expected error when featuregate is absent, got nil")
		}
	})
}

func newFakeCRDClient(t *testing.T, crd *unstructured.Unstructured) *fake.FakeDynamicClient {
	t.Helper()
	var objs []runtime.Object
	if crd != nil {
		objs = append(objs, crd)
	}
	return fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{crdTestGVR: crdTestListKind},
		objs...)
}

func makeCRDObject(conds []any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": crdTestName},
		"status":     map[string]any{"conditions": conds},
	}}
}

func TestWaitForCRDEstablished(t *testing.T) {
	poll, timeout := 10*time.Millisecond, 50*time.Millisecond

	t.Run("established: returns nil immediately", func(t *testing.T) {
		crd := makeCRDObject([]any{map[string]any{"type": "Established", "status": "True"}})
		if err := waitForCRDEstablished(newFakeCRDClient(t, crd), crdTestName, poll, timeout); err != nil {
			t.Fatalf("expected success for established CRD, got %v", err)
		}
	})

	t.Run("established among other conditions: returns nil", func(t *testing.T) {
		crd := makeCRDObject([]any{
			map[string]any{"type": "NamesAccepted", "status": "True"},
			map[string]any{"type": "Established", "status": "True"},
			map[string]any{"type": "Terminating", "status": "False"},
		})
		if err := waitForCRDEstablished(newFakeCRDClient(t, crd), crdTestName, poll, timeout); err != nil {
			t.Fatalf("expected success, got %v", err)
		}
	})

	t.Run("established=False: times out", func(t *testing.T) {
		crd := makeCRDObject([]any{
			map[string]any{"type": "Established", "status": "False"},
			map[string]any{"type": "NamesAccepted", "status": "True"},
		})
		if err := waitForCRDEstablished(newFakeCRDClient(t, crd), crdTestName, poll, timeout); err == nil {
			t.Fatal("expected timeout error for Established=False, got nil")
		}
	})

	t.Run("absent CRD: times out", func(t *testing.T) {
		if err := waitForCRDEstablished(newFakeCRDClient(t, nil), crdTestName, poll, timeout); err == nil {
			t.Fatal("expected timeout error when CRD is absent, got nil")
		}
	})

	t.Run("CRD without status: times out", func(t *testing.T) {
		crd := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata":   map[string]any{"name": crdTestName},
		}}
		if err := waitForCRDEstablished(newFakeCRDClient(t, crd), crdTestName, poll, timeout); err == nil {
			t.Fatal("expected timeout error for CRD without conditions, got nil")
		}
	})

	t.Run("status value is case-sensitive", func(t *testing.T) {
		crd := makeCRDObject([]any{map[string]any{"type": "Established", "status": "true"}})
		if err := waitForCRDEstablished(newFakeCRDClient(t, crd), crdTestName, poll, timeout); err == nil {
			t.Fatal("expected timeout error for lowercase status, got nil")
		}
	})

	t.Run("non-NotFound API error: fails fast", func(t *testing.T) {
		client := newFakeCRDClient(t, nil)
		client.PrependReactor("get", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("boom: connection refused")
		})
		start := time.Now()
		err := waitForCRDEstablished(client, crdTestName, poll, time.Minute)
		if err == nil {
			t.Fatal("expected error on API failure, got nil")
		}
		if time.Since(start) > 2*time.Second {
			t.Fatalf("expected fast failure on API error, took %v", time.Since(start))
		}
	})

	t.Run("NotFound keeps polling until timeout", func(t *testing.T) {
		client := newFakeCRDClient(t, nil)
		client.PrependReactor("get", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.NewNotFound(
				schema.GroupResource{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"},
				crdTestName)
		})
		if err := waitForCRDEstablished(client, crdTestName, poll, timeout); err == nil {
			t.Fatal("expected timeout error on NotFound, got nil")
		}
	})
}
