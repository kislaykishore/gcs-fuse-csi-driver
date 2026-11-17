/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testsuites

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"local/test/e2e/specs"

	"github.com/google/go-cmp/cmp"
	csidriver "github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/csi_driver"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestValidateMemorySoakConfig(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		duration       time.Duration
		scrapeInterval time.Duration
		podCount       int
		memLimit       string
		wantLimitBytes int64
		wantErr        bool
	}{
		{
			name:           "valid default config",
			duration:       4 * time.Hour,
			scrapeInterval: 60 * time.Second,
			podCount:       80,
			memLimit:       MemorySoakCSIDriverMemoryLimit,
			wantLimitBytes: 50 * 1024 * 1024,
		},
		{
			name:           "valid empty memory limit disables override",
			duration:       10 * time.Minute,
			scrapeInterval: 30 * time.Second,
			podCount:       1,
			memLimit:       "",
			wantLimitBytes: 0,
		},
		{
			name:           "invalid zero pod count",
			duration:       time.Hour,
			scrapeInterval: time.Minute,
			podCount:       0,
			memLimit:       "100Mi",
			wantErr:        true,
		},
		{
			name:           "invalid negative duration",
			duration:       -1 * time.Second,
			scrapeInterval: time.Minute,
			podCount:       10,
			memLimit:       "100Mi",
			wantErr:        true,
		},
		{
			name:           "invalid zero scrape interval",
			duration:       time.Hour,
			scrapeInterval: 0,
			podCount:       10,
			memLimit:       "100Mi",
			wantErr:        true,
		},
		{
			name:           "invalid memory limit syntax",
			duration:       time.Hour,
			scrapeInterval: time.Minute,
			podCount:       10,
			memLimit:       "not-a-quantity",
			wantErr:        true,
		},
		{
			name:           "invalid zero memory limit",
			duration:       time.Hour,
			scrapeInterval: time.Minute,
			podCount:       10,
			memLimit:       "0",
			wantErr:        true,
		},
		{
			name:           "invalid negative memory limit",
			duration:       time.Hour,
			scrapeInterval: time.Minute,
			podCount:       10,
			memLimit:       "-50Mi",
			wantErr:        true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			qty, err := validateMemorySoakConfig(tc.duration, tc.scrapeInterval, tc.podCount, tc.memLimit)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (qty=%s)", qty.String())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if qty.Value() != tc.wantLimitBytes {
				t.Errorf("got limit bytes %d, want %d", qty.Value(), tc.wantLimitBytes)
			}
		})
	}
}

func TestSplitSoakPodCounts(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		total      int
		wantDeploy int
		wantSts    int
		wantJob    int
	}{
		{total: 0, wantDeploy: 0, wantSts: 0, wantJob: 0},
		{total: 1, wantDeploy: 1, wantSts: 0, wantJob: 0},
		{total: 2, wantDeploy: 2, wantSts: 0, wantJob: 0},
		{total: 3, wantDeploy: 1, wantSts: 1, wantJob: 1},
		{total: 8, wantDeploy: 6, wantSts: 1, wantJob: 1},
		{total: 16, wantDeploy: 12, wantSts: 2, wantJob: 2},
		{total: 80, wantDeploy: 60, wantSts: 10, wantJob: 10},
		{total: 100, wantDeploy: 76, wantSts: 12, wantJob: 12},
	}

	for _, tc := range testCases {
		d, s, j := splitSoakPodCounts(tc.total)
		if d != tc.wantDeploy || s != tc.wantSts || j != tc.wantJob {
			t.Errorf("splitSoakPodCounts(%d) = (%d, %d, %d), want (%d, %d, %d)",
				tc.total, d, s, j, tc.wantDeploy, tc.wantSts, tc.wantJob)
		}
		if d+s+j != tc.total {
			t.Errorf("splitSoakPodCounts(%d) sum = %d, want %d", tc.total, d+s+j, tc.total)
		}
	}
}

func TestComputeSoakWarmupDuration(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		soak time.Duration
		want time.Duration
	}{
		{soak: 4 * time.Hour, want: 2 * time.Minute},
		{soak: 2 * time.Minute, want: 2 * time.Minute},
		{soak: 90 * time.Second, want: 30 * time.Second},
		{soak: 60 * time.Second, want: 30 * time.Second},
		{soak: 30 * time.Second, want: 15 * time.Second},
		{soak: 10 * time.Second, want: 5 * time.Second},
	}

	for _, tc := range testCases {
		if got := computeSoakWarmupDuration(tc.soak); got != tc.want {
			t.Errorf("computeSoakWarmupDuration(%s) = %s, want %s", tc.soak, got, tc.want)
		}
	}
}

func TestBuildMemorySoakMountOptions(t *testing.T) {
	t.Parallel()

	httpOpts := buildMemorySoakMountOptions("http1")
	for _, want := range []string{
		"implicit-dirs",
		"file-cache:max-size-mb:20",
		"file-cache:cache-file-for-range-read:true",
		"metadata-cache:ttl-secs:60",
		"metadata-cache:stat-cache-max-size-mb:4",
		"metadata-cache:type-cache-max-size-mb:4",
	} {
		if !strings.Contains(httpOpts, want) {
			t.Errorf("http1 mount options %q missing %q", httpOpts, want)
		}
	}
	if strings.Contains(httpOpts, "client-protocol=grpc") || strings.Contains(httpOpts, "experimental-enable-grpc-metrics") {
		t.Errorf("http1 mount options %q unexpectedly included gRPC flags", httpOpts)
	}

	grpcOpts := buildMemorySoakMountOptions("grpc")
	for _, want := range []string{
		"implicit-dirs",
		"file-cache:max-size-mb:20",
		"client-protocol=grpc",
		"metrics:experimental-enable-grpc-metrics:true",
	} {
		if !strings.Contains(grpcOpts, want) {
			t.Errorf("grpc mount options %q missing %q", grpcOpts, want)
		}
	}
}

func TestSelectSoakTargetNode(t *testing.T) {
	t.Parallel()

	makeNode := func(name string, ready, unschedulable bool, osLabel string, taints []corev1.Taint) corev1.Node {
		condStatus := corev1.ConditionFalse
		if ready {
			condStatus = corev1.ConditionTrue
		}
		return corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: map[string]string{"kubernetes.io/os": osLabel},
			},
			Spec: corev1.NodeSpec{
				Unschedulable: unschedulable,
				Taints:        taints,
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: condStatus},
				},
			},
		}
	}

	nodes := []corev1.Node{
		makeNode("node-unsched", true, true, "linux", nil),
		makeNode("node-notready", false, false, "linux", nil),
		makeNode("node-win", true, false, "windows", nil),
		makeNode("node-tainted", true, false, "linux", []corev1.Taint{{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule}}),
		makeNode("node-no-csi", true, false, "linux", nil),
		makeNode("node-b", true, false, "linux", nil),
		makeNode("node-a", true, false, "linux", nil),
	}
	csiNodes := map[string]bool{
		"node-unsched":  true,
		"node-notready": true,
		"node-tainted":  true,
		"node-b":        true,
		"node-a":        true,
	}

	got, err := selectSoakTargetNode(nodes, csiNodes)
	if err != nil {
		t.Fatalf("selectSoakTargetNode failed: %v", err)
	}
	if got != "node-a" {
		t.Errorf("got node %q, want %q", got, "node-a")
	}

	// Falls back to tainted node if no untainted Ready node has CSI pod.
	gotFallback, err := selectSoakTargetNode(nodes, map[string]bool{"node-tainted": true})
	if err != nil {
		t.Fatalf("selectSoakTargetNode fallback failed: %v", err)
	}
	if gotFallback != "node-tainted" {
		t.Errorf("got fallback node %q, want %q", gotFallback, "node-tainted")
	}

	// Errors if no qualifying node exists.
	if _, err := selectSoakTargetNode(nodes, map[string]bool{"node-notready": true}); err == nil {
		t.Error("expected error when no Ready schedulable node runs CSI pod, got nil")
	}
}

func TestCheckCSIContainerHealth(t *testing.T) {
	t.Parallel()

	newHealthyPod := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "gcsfusecsi-node-abcde",
				Namespace: "gcs-fuse-csi-driver",
				UID:       types.UID("uid-initial"),
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: memorySoakCSIContainerName,
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("100Mi"),
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:         memorySoakCSIContainerName,
						Ready:        true,
						RestartCount: 0,
						State: corev1.ContainerState{
							Running: &corev1.ContainerStateRunning{},
						},
					},
				},
			},
		}
	}

	testCases := []struct {
		name         string
		mutate       func(*corev1.Pod) *corev1.Pod
		expectedUID  types.UID
		expectedRest int32
		wantErrSub   string
	}{
		{
			name:         "healthy container passes",
			mutate:       func(p *corev1.Pod) *corev1.Pod { return p },
			expectedUID:  "uid-initial",
			expectedRest: 0,
		},
		{
			name:         "nil pod fails",
			mutate:       func(_ *corev1.Pod) *corev1.Pod { return nil },
			expectedUID:  "uid-initial",
			expectedRest: 0,
			wantErrSub:   "nil",
		},
		{
			name: "UID mismatch fails",
			mutate: func(p *corev1.Pod) *corev1.Pod {
				p.UID = "uid-new"
				return p
			},
			expectedUID:  "uid-initial",
			expectedRest: 0,
			wantErrSub:   "recreated",
		},
		{
			name: "restartCount increase fails",
			mutate: func(p *corev1.Pod) *corev1.Pod {
				p.Status.ContainerStatuses[0].RestartCount = 1
				return p
			},
			expectedUID:  "uid-initial",
			expectedRest: 0,
			wantErrSub:   "restarted",
		},
		{
			name: "LastTerminationState OOMKilled fails",
			mutate: func(p *corev1.Pod) *corev1.Pod {
				p.Status.ContainerStatuses[0].RestartCount = 1
				p.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason:   "OOMKilled",
						ExitCode: 137,
					},
				}
				return p
			},
			expectedUID:  "uid-initial",
			expectedRest: 0,
			wantErrSub:   "OOMKilled",
		},
		{
			name: "State Terminated exit code 137 fails",
			mutate: func(p *corev1.Pod) *corev1.Pod {
				p.Status.ContainerStatuses[0].Ready = false
				p.Status.ContainerStatuses[0].State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason:   "Error",
						ExitCode: 137,
					},
				}
				return p
			},
			expectedUID:  "uid-initial",
			expectedRest: 0,
			wantErrSub:   "exitCode=137",
		},
		{
			name: "container not ready fails",
			mutate: func(p *corev1.Pod) *corev1.Pod {
				p.Status.ContainerStatuses[0].Ready = false
				return p
			},
			expectedUID:  "uid-initial",
			expectedRest: 0,
			wantErrSub:   "not ready",
		},
		{
			name: "pod terminating fails",
			mutate: func(p *corev1.Pod) *corev1.Pod {
				now := metav1.Now()
				p.DeletionTimestamp = &now
				return p
			},
			expectedUID:  "uid-initial",
			expectedRest: 0,
			wantErrSub:   "terminating",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pod := tc.mutate(newHealthyPod())
			err := checkCSIContainerHealth(pod, tc.expectedUID, tc.expectedRest)
			if tc.wantErrSub == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErrSub)
			}
			if !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErrSub)
			}
		})
	}
}

func TestFindCSIOOMKillingEvents(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	events := []corev1.Event{
		{
			Reason:         "OOMKilling",
			InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: "node-a"},
			LastTimestamp:  metav1.NewTime(base.Add(-5 * time.Minute)),
			Message:        "Memory cgroup out of memory: Killed process 100 (gcs-fuse-csi-dr)",
		},
		{
			Reason:         "Started",
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "gcsfusecsi-node-1234"},
			LastTimestamp:  metav1.NewTime(base.Add(1 * time.Minute)),
			Message:        "Started container gcs-fuse-csi-driver",
		},
		{
			Reason:         "OOMKilling",
			InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: "node-a"},
			LastTimestamp:  metav1.NewTime(base.Add(2 * time.Minute)),
			Message:        "Memory cgroup out of memory: Killed process 200 (unrelated-app)",
		},
		{
			Reason:         "OOMKilling",
			InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: "node-other"},
			LastTimestamp:  metav1.NewTime(base.Add(3 * time.Minute)),
			Message:        "Memory cgroup out of memory: Killed process 300 (gcs-fuse-csi-dr)",
		},
		{
			Reason:         "OOMKilling",
			InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: "node-a"},
			LastTimestamp:  metav1.NewTime(base.Add(4 * time.Minute)),
			Message:        "Memory cgroup out of memory: Killed process 400 (gcs-fuse-csi-dr) total-vm:1234kB",
		},
		{
			Reason:         "OOMKilling",
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "gcsfusecsi-node-1234"},
			EventTime:      metav1.NewMicroTime(base.Add(5 * time.Minute)),
			Message:        "Container gcs-fuse-csi-driver OOMKilled",
		},
		{
			// Series event whose initial EventTime predates `since`, while LastObservedTime is after `since`.
			Reason:         "OOMKilling",
			InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: "node-a"},
			EventTime:      metav1.NewMicroTime(base.Add(-15 * time.Minute)),
			Series: &corev1.EventSeries{
				Count:            2,
				LastObservedTime: metav1.NewMicroTime(base.Add(6 * time.Minute)),
			},
			Message: "Memory cgroup out of memory: Killed process 500 (gcs-fuse-csi-dr) series repeat",
		},
		{
			// Event within 5s clock-skew buffer before `since` should still be captured.
			Reason:         "OOMKilling",
			InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: "node-a"},
			LastTimestamp:  metav1.NewTime(base.Add(-3 * time.Second)),
			Message:        "Memory cgroup out of memory: Killed process 600 (gcs-fuse-csi-dr) clock-skew",
		},
		{
			// Event older than 5s clock-skew buffer before `since` should be ignored.
			Reason:         "OOMKilling",
			InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: "node-a"},
			LastTimestamp:  metav1.NewTime(base.Add(-10 * time.Second)),
			Message:        "Memory cgroup out of memory: Killed process 700 (gcs-fuse-csi-dr) too-old",
		},
	}

	got := findCSIOOMKillingEvents(events, "gcsfusecsi-node-1234", "node-a", base)
	if len(got) != 4 {
		t.Fatalf("got %d OOMKilling matches (%v), want 4", len(got), got)
	}
	if !strings.Contains(got[0], "gcs-fuse-csi-dr") {
		t.Errorf("match[0] = %q, expected truncated process name match", got[0])
	}
	if !strings.Contains(got[1], "gcsfusecsi-node-1234") {
		t.Errorf("match[1] = %q, expected pod involvedObject match", got[1])
	}
	if !strings.Contains(got[2], "series repeat") {
		t.Errorf("match[2] = %q, expected EventSeries.LastObservedTime match", got[2])
	}
	if !strings.Contains(got[3], "clock-skew") {
		t.Errorf("match[3] = %q, expected 5s clock-skew buffer match", got[3])
	}
}

func TestCheckWorkloadPodsHealth(t *testing.T) {
	t.Parallel()

	makeReadyPod := func(name, node string) corev1.Pod {
		return corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       corev1.PodSpec{NodeName: node},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
	}

	pods := []corev1.Pod{
		makeReadyPod("pod-1", "node-a"),
		makeReadyPod("pod-2", "node-a"),
	}
	if err := checkWorkloadPodsHealth(pods, 2, "node-a"); err != nil {
		t.Fatalf("expected healthy pods to pass, got: %v", err)
	}
	if err := checkWorkloadPodsHealth(pods, 3, "node-a"); err == nil {
		t.Error("expected error on pod count mismatch, got nil")
	}

	wrongNode := []corev1.Pod{
		makeReadyPod("pod-1", "node-a"),
		makeReadyPod("pod-2", "node-b"),
	}
	if err := checkWorkloadPodsHealth(wrongNode, 2, "node-a"); err == nil {
		t.Error("expected error when workload pod is on wrong node, got nil")
	}

	oomPod := makeReadyPod("pod-2", "node-a")
	oomPod.Status.InitContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "gke-gcsfuse-sidecar",
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137},
			},
		},
	}
	if err := checkWorkloadPodsHealth([]corev1.Pod{makeReadyPod("pod-1", "node-a"), oomPod}, 2, "node-a"); err == nil {
		t.Error("expected error when workload sidecar was OOMKilled, got nil")
	}
}

func TestAnalyzeScrapePayload(t *testing.T) {
	t.Parallel()

	payload := strings.Join([]string{
		`# HELP volume_fs_ops_count Total number of fs ops`,
		`# TYPE volume_fs_ops_count counter`,
		`volume_fs_ops_count{bucket_name="b1",fs_op="ReadFile",k8s_controller_name="gcsfuse-soak-deploy",k8s_controller_type="deployment",namespace_name="soak-ns",pod_name="pod-1",volume_name="gcsfuse-vol"} 12`,
		`grpc_client_attempt_started_total{grpc_method="/google.storage.v2.Storage/ReadObject",namespace_name="soak-ns",pod_name="pod-1"} 4`,
		`volume_fs_ops_count{bucket_name="b1",desc="escaped \"quote\", comma",namespace_name="soak-ns",pod_name="pod-2",volume_name="gcsfuse-vol"} 9`,
		`volume_fs_ops_count{bucket_name="other",namespace_name="other-ns",pod_name="pod-3",volume_name="gcsfuse-vol"} 5`,
		`go_goroutines 42`,
	}, "\n")

	expected := map[string]struct{}{
		"pod-1": {},
		"pod-2": {},
		"pod-3": {},
	}

	got := analyzeScrapePayload([]byte(payload), "soak-ns", expected)
	if got.Bytes != len(payload) {
		t.Errorf("got Bytes=%d, want %d", got.Bytes, len(payload))
	}
	if got.SeriesCount != 5 {
		t.Errorf("got SeriesCount=%d, want 5", got.SeriesCount)
	}
	if got.DistinctPods != 2 {
		t.Errorf("got DistinctPods=%d, want 2", got.DistinctPods)
	}
	if diff := cmp.Diff([]string{"pod-3"}, got.MissingPods); diff != "" {
		t.Errorf("MissingPods mismatch (-want +got):\n%s", diff)
	}

	empty := analyzeScrapePayload(nil, "soak-ns", expected)
	if empty.Bytes != 0 || empty.SeriesCount != 0 || empty.DistinctPods != 0 {
		t.Errorf("unexpected non-zero result for empty payload: %+v", empty)
	}
}

func TestParseStatsSummaryMemory(t *testing.T) {
	t.Parallel()

	sampleJSON := `{
  "node": {"nodeName": "node-a"},
  "pods": [
    {
      "podRef": {"name": "other-pod", "namespace": "default"},
      "containers": [
        {"name": "gcs-fuse-csi-driver", "memory": {"workingSetBytes": 999999, "rssBytes": 888888}}
      ]
    },
    {
      "podRef": {"name": "gcsfusecsi-node-abcde", "namespace": "gcs-fuse-csi-driver"},
      "containers": [
        {"name": "csi-driver-registrar", "memory": {"workingSetBytes": 12582912, "rssBytes": 10485760}},
        {"name": "gcs-fuse-csi-driver", "memory": {"workingSetBytes": 62914560, "rssBytes": 57671680}}
      ]
    }
  ]
}`

	ws, rss, found, err := parseStatsSummaryMemory([]byte(sampleJSON), "gcs-fuse-csi-driver", "gcsfusecsi-node-abcde", memorySoakCSIContainerName)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected container memory to be found")
	}
	if ws != 62914560 || rss != 57671680 {
		t.Errorf("got ws=%d rss=%d, want ws=62914560 rss=57671680", ws, rss)
	}

	_, _, found, err = parseStatsSummaryMemory([]byte(sampleJSON), "kube-system", "gcsfusecsi-node-abcde", memorySoakCSIContainerName)
	if err != nil {
		t.Fatalf("unexpected error on namespace miss: %v", err)
	}
	if found {
		t.Error("expected found=false when namespace does not match")
	}

	if _, _, _, err := parseStatsSummaryMemory([]byte("{invalid"), "gcs-fuse-csi-driver", "gcsfusecsi-node-abcde", memorySoakCSIContainerName); err == nil {
		t.Error("expected error for malformed JSON, got nil")
	}
}

func TestParseCadvisorMemoryPeak(t *testing.T) {
	t.Parallel()

	cadvisorText := strings.Join([]string{
		`# HELP container_memory_max_usage_bytes Maximum memory usage recorded in bytes`,
		`# TYPE container_memory_max_usage_bytes gauge`,
		`container_memory_working_set_bytes{container="gcs-fuse-csi-driver",namespace="gcs-fuse-csi-driver",pod="gcsfusecsi-node-abcde"} 9.9e+08`,
		`container_memory_max_usage_bytes{container="csi-driver-registrar",id="/kubepods/pod1/c1",namespace="gcs-fuse-csi-driver",pod="gcsfusecsi-node-abcde"} 1.5e+07 1700000000000`,
		`container_memory_max_usage_bytes{container="gcs-fuse-csi-driver",id="/kubepods.slice/kubepods-burstable.slice/pod,1",image="gcr.io/test:v1",namespace="gcs-fuse-csi-driver",pod="gcsfusecsi-node-abcde"} 7.5e+07 1700000000000`,
		`container_memory_max_usage_bytes{container="gcs-fuse-csi-driver",id="/kubepods.slice/kubepods-burstable.slice/pod,1",image="gcr.io/test:v1",namespace="gcs-fuse-csi-driver",pod="gcsfusecsi-node-abcde"} 8.54321e+07 1700000060000`,
		`container_memory_max_usage_bytes{container="gcs-fuse-csi-driver",namespace="kube-system",pod="gcsfusecsi-node-abcde"} 9.9e+07`,
	}, "\n")

	peak, found := parseCadvisorMemoryPeak([]byte(cadvisorText), "gcs-fuse-csi-driver", "gcsfusecsi-node-abcde", memorySoakCSIContainerName)
	if !found {
		t.Fatal("expected peak to be found")
	}
	if peak != 85432100 {
		t.Errorf("got peak=%d, want 85432100", peak)
	}

	if _, found := parseCadvisorMemoryPeak([]byte(cadvisorText), "gcs-fuse-csi-driver", "gcsfusecsi-node-other", memorySoakCSIContainerName); found {
		t.Error("expected found=false for non-matching pod")
	}
}

func makeTestCSIPod(ns, name, node string, uid types.UID, phase corev1.PodPhase, created time.Time, memLimit string, ready bool, restarts int32) *corev1.Pod {
	var limits corev1.ResourceList
	if memLimit != "" {
		limits = corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse(memLimit),
		}
	}
	condStatus := corev1.ConditionFalse
	if ready {
		condStatus = corev1.ConditionTrue
	}
	var containerState corev1.ContainerState
	if phase == corev1.PodRunning {
		containerState.Running = &corev1.ContainerStateRunning{}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         ns,
			UID:               uid,
			CreationTimestamp: metav1.NewTime(created),
			Labels:            map[string]string{"k8s-app": "gcs-fuse-csi-driver"},
		},
		Spec: corev1.PodSpec{
			NodeName: node,
			Containers: []corev1.Container{
				{
					Name: memorySoakCSIContainerName,
					Resources: corev1.ResourceRequirements{
						Limits: limits,
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: phase,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: condStatus},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:         memorySoakCSIContainerName,
					Ready:        ready,
					RestartCount: restarts,
					State:        containerState,
				},
			},
		},
	}
}

func makeTestCSIDaemonSet(ns, name string, gen int64, memLimit, memRequest, gomemlimit string) *appsv1.DaemonSet {
	limits := corev1.ResourceList{}
	if memLimit != "" {
		limits[corev1.ResourceMemory] = resource.MustParse(memLimit)
	}
	requests := corev1.ResourceList{}
	if memRequest != "" {
		requests[corev1.ResourceMemory] = resource.MustParse(memRequest)
	}
	var env []corev1.EnvVar
	if gomemlimit != "" {
		env = append(env, corev1.EnvVar{Name: "GOMEMLIMIT", Value: gomemlimit})
	}
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  ns,
			Generation: gen,
		},
		Spec: appsv1.DaemonSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: memorySoakCSIContainerName,
							Env:  env,
							Resources: corev1.ResourceRequirements{
								Limits:   limits,
								Requests: requests,
							},
						},
					},
				},
			},
		},
		Status: appsv1.DaemonSetStatus{
			ObservedGeneration:     gen,
			DesiredNumberScheduled: 1,
			UpdatedNumberScheduled: 1,
			NumberAvailable:        1,
			NumberUnavailable:      0,
		},
	}
}

func TestEnsureCSIDriverMemoryLimit(t *testing.T) {
	origSettle := memorySoakRolloutSettleDuration
	origPoll := memorySoakRolloutPollInterval
	origAPIRetry := memorySoakAPIRetryDelay
	memorySoakRolloutSettleDuration = time.Millisecond
	memorySoakRolloutPollInterval = time.Millisecond
	memorySoakAPIRetryDelay = time.Millisecond
	t.Cleanup(func() {
		memorySoakRolloutSettleDuration = origSettle
		memorySoakRolloutPollInterval = origPoll
		memorySoakAPIRetryDelay = origAPIRetry
	})

	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	t.Run("empty requested limit is a no-op", func(t *testing.T) {
		pod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-1", "node-a", "uid-1", corev1.PodRunning, now, "256Mi", true, 0)
		client := fake.NewSimpleClientset(pod)

		gotPod, gotLimit, err := ensureCSIDriverMemoryLimit(ctx, client, "gcs-fuse-csi-driver", "gcsfusecsi-node", "node-a", pod, "", resource.Quantity{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gotPod.Name != pod.Name || gotLimit != "256Mi" {
			t.Errorf("got (%q, %q), want (%q, %q)", gotPod.Name, gotLimit, pod.Name, "256Mi")
		}
		if len(client.Actions()) != 0 {
			t.Errorf("expected no Kubernetes API actions, got %d", len(client.Actions()))
		}
	})

	t.Run("kube-system managed driver skips DaemonSet patch", func(t *testing.T) {
		pod := makeTestCSIPod(memorySoakManagedNamespace, "gcsfusecsi-node-1", "node-a", "uid-1", corev1.PodRunning, now, "256Mi", true, 0)
		ds := makeTestCSIDaemonSet(memorySoakManagedNamespace, "gcsfusecsi-node", 1, "256Mi", "64Mi", "200MiB")
		client := fake.NewSimpleClientset(pod, ds)

		gotPod, gotLimit, err := ensureCSIDriverMemoryLimit(ctx, client, memorySoakManagedNamespace, "gcsfusecsi-node", "node-a", pod, "100Mi", resource.MustParse("100Mi"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gotPod.Name != pod.Name || gotLimit != "256Mi" {
			t.Errorf("got (%q, %q), want (%q, %q)", gotPod.Name, gotLimit, pod.Name, "256Mi")
		}
		for _, a := range client.Actions() {
			if a.GetVerb() == "update" {
				t.Errorf("unexpected update action on managed kube-system DaemonSet: %+v", a)
			}
		}
	})

	t.Run("already-matching limit is a no-op without DaemonSet update", func(t *testing.T) {
		pod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-1", "node-a", "uid-1", corev1.PodRunning, now, "100Mi", true, 0)
		ds := makeTestCSIDaemonSet("gcs-fuse-csi-driver", "gcsfusecsi-node", 1, "100Mi", "64Mi", "90MiB")
		client := fake.NewSimpleClientset(pod, ds)

		gotPod, gotLimit, err := ensureCSIDriverMemoryLimit(ctx, client, "gcs-fuse-csi-driver", "gcsfusecsi-node", "node-a", pod, "100Mi", resource.MustParse("100Mi"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gotPod.UID != "uid-1" || gotLimit != "100Mi" {
			t.Errorf("got (UID=%s, limit=%q), want (uid-1, 100Mi)", gotPod.UID, gotLimit)
		}
		for _, a := range client.Actions() {
			if a.GetVerb() == "update" && a.GetResource().Resource == "daemonsets" {
				t.Errorf("unexpected DaemonSet update when limit already matches: %+v", a)
			}
		}
	})

	t.Run("patches non-managed DaemonSet, clamps Requests, preserves GOMEMLIMIT, and refreshes post-settle pod", func(t *testing.T) {
		oldPod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-old", "node-a", "uid-old", corev1.PodRunning, now, "256Mi", true, 0)
		ds := makeTestCSIDaemonSet("gcs-fuse-csi-driver", "gcsfusecsi-node", 1, "256Mi", "150Mi", "200MiB")
		client := fake.NewSimpleClientset(oldPod, ds)

		client.PrependReactor("update", "daemonsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
			upd := action.(k8stesting.UpdateAction).GetObject().(*appsv1.DaemonSet)
			upd.Generation++
			upd.Status.ObservedGeneration = upd.Generation
			upd.Status.DesiredNumberScheduled = 1
			upd.Status.UpdatedNumberScheduled = 1
			upd.Status.NumberAvailable = 1
			upd.Status.NumberUnavailable = 0

			newLim := upd.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory]
			newPod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-new", "node-a", "uid-new", corev1.PodRunning, now.Add(time.Second), newLim.String(), true, 0)
			_ = client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "gcs-fuse-csi-driver", "gcsfusecsi-node-old")
			_ = client.Tracker().Add(newPod)
			return false, nil, nil
		})

		gotPod, gotLimit, err := ensureCSIDriverMemoryLimit(ctx, client, "gcs-fuse-csi-driver", "gcsfusecsi-node", "node-a", oldPod, "100Mi", resource.MustParse("100Mi"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gotPod.Name != "gcsfusecsi-node-new" || gotPod.UID != "uid-new" || gotLimit != "100Mi" {
			t.Errorf("got (%q, %s, %q), want (gcsfusecsi-node-new, uid-new, 100Mi)", gotPod.Name, gotPod.UID, gotLimit)
		}

		updatedDS, err := client.AppsV1().DaemonSets("gcs-fuse-csi-driver").Get(ctx, "gcsfusecsi-node", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get updated DaemonSet: %v", err)
		}
		c := updatedDS.Spec.Template.Spec.Containers[0]
		if got := c.Resources.Limits[corev1.ResourceMemory]; got.Cmp(resource.MustParse("100Mi")) != 0 {
			t.Errorf("DaemonSet memory limit = %s, want 100Mi", got.String())
		}
		if got := c.Resources.Requests[corev1.ResourceMemory]; got.Cmp(resource.MustParse("100Mi")) != 0 {
			t.Errorf("DaemonSet memory request = %s, want clamped to 100Mi", got.String())
		}
		if len(c.Env) != 1 || c.Env[0].Name != "GOMEMLIMIT" || c.Env[0].Value != "200MiB" {
			t.Errorf("expected GOMEMLIMIT env untouched, got %+v", c.Env)
		}
	})

	t.Run("detects OOMKill during post-settle refresh", func(t *testing.T) {
		oldPod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-old", "node-a", "uid-old", corev1.PodRunning, now, "256Mi", true, 0)
		ds := makeTestCSIDaemonSet("gcs-fuse-csi-driver", "gcsfusecsi-node", 1, "256Mi", "64Mi", "")
		client := fake.NewSimpleClientset(oldPod, ds)

		client.PrependReactor("update", "daemonsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
			upd := action.(k8stesting.UpdateAction).GetObject().(*appsv1.DaemonSet)
			upd.Generation++
			upd.Status.ObservedGeneration = upd.Generation
			upd.Status.DesiredNumberScheduled = 1
			upd.Status.UpdatedNumberScheduled = 1
			upd.Status.NumberAvailable = 1
			upd.Status.NumberUnavailable = 0
			return false, nil, nil
		})

		var podListCalls atomic.Int32
		client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			call := podListCalls.Add(1)
			p := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-new", "node-a", "uid-new", corev1.PodRunning, now.Add(time.Second), "100Mi", true, 0)
			if call >= 2 {
				// Simulate OOMKill happening during the post-settle window.
				p.Status.ContainerStatuses[0].RestartCount = 1
				p.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137},
				}
			}
			return true, &corev1.PodList{Items: []corev1.Pod{*p}}, nil
		})

		_, _, err := ensureCSIDriverMemoryLimit(ctx, client, "gcs-fuse-csi-driver", "gcsfusecsi-node", "node-a", oldPod, "100Mi", resource.MustParse("100Mi"))
		if err == nil || !strings.Contains(err.Error(), "OOMKilled") {
			t.Fatalf("expected OOMKilled error after rollout settle, got: %v", err)
		}
	})

	t.Run("detects pod UID change during post-settle refresh", func(t *testing.T) {
		oldPod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-old", "node-a", "uid-old", corev1.PodRunning, now, "256Mi", true, 0)
		ds := makeTestCSIDaemonSet("gcs-fuse-csi-driver", "gcsfusecsi-node", 1, "256Mi", "64Mi", "")
		client := fake.NewSimpleClientset(oldPod, ds)

		client.PrependReactor("update", "daemonsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
			upd := action.(k8stesting.UpdateAction).GetObject().(*appsv1.DaemonSet)
			upd.Generation++
			upd.Status.ObservedGeneration = upd.Generation
			upd.Status.DesiredNumberScheduled = 1
			upd.Status.UpdatedNumberScheduled = 1
			upd.Status.NumberAvailable = 1
			upd.Status.NumberUnavailable = 0
			return false, nil, nil
		})

		var podListCalls atomic.Int32
		client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			call := podListCalls.Add(1)
			uid := types.UID("uid-new")
			if call >= 2 {
				uid = types.UID("uid-recreated")
			}
			p := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-new", "node-a", uid, corev1.PodRunning, now.Add(time.Second), "100Mi", true, 0)
			return true, &corev1.PodList{Items: []corev1.Pod{*p}}, nil
		})

		_, _, err := ensureCSIDriverMemoryLimit(ctx, client, "gcs-fuse-csi-driver", "gcsfusecsi-node", "node-a", oldPod, "100Mi", resource.MustParse("100Mi"))
		if err == nil || !strings.Contains(err.Error(), "changed UID") {
			t.Fatalf("expected UID change error after rollout settle, got: %v", err)
		}
	})

	t.Run("runs post-settle verification when DaemonSet already matches limit but pod rolls over (catches OOMKill)", func(t *testing.T) {
		oldPod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-old", "node-a", "uid-old", corev1.PodRunning, now, "256Mi", true, 0)
		ds := makeTestCSIDaemonSet("gcs-fuse-csi-driver", "gcsfusecsi-node", 2, "100Mi", "64Mi", "")
		client := fake.NewSimpleClientset(oldPod, ds)

		var podListCalls atomic.Int32
		client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			call := podListCalls.Add(1)
			p := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-new", "node-a", "uid-new", corev1.PodRunning, now.Add(time.Second), "100Mi", true, 0)
			if call >= 2 {
				p.Status.ContainerStatuses[0].RestartCount = 1
				p.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137},
				}
			}
			return true, &corev1.PodList{Items: []corev1.Pod{*p}}, nil
		})

		_, _, err := ensureCSIDriverMemoryLimit(ctx, client, "gcs-fuse-csi-driver", "gcsfusecsi-node", "node-a", oldPod, "100Mi", resource.MustParse("100Mi"))
		if err == nil || !strings.Contains(err.Error(), "OOMKilled") {
			t.Fatalf("expected OOMKilled error during post-settle verification after pod rollover, got: %v", err)
		}
		if podListCalls.Load() != 2 {
			t.Errorf("got %d pod list calls, want 2 (rollout poll + post-settle refresh)", podListCalls.Load())
		}
		for _, a := range client.Actions() {
			if a.GetVerb() == "update" && a.GetResource().Resource == "daemonsets" {
				t.Errorf("unexpected DaemonSet update when DaemonSet already matches desired limit: %+v", a)
			}
		}
	})

	t.Run("runs post-settle verification when DaemonSet already matches limit but pod rolls over (catches UID change)", func(t *testing.T) {
		oldPod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-old", "node-a", "uid-old", corev1.PodRunning, now, "256Mi", true, 0)
		ds := makeTestCSIDaemonSet("gcs-fuse-csi-driver", "gcsfusecsi-node", 2, "100Mi", "64Mi", "")
		client := fake.NewSimpleClientset(oldPod, ds)

		var podListCalls atomic.Int32
		client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			call := podListCalls.Add(1)
			uid := types.UID("uid-new")
			if call >= 2 {
				uid = types.UID("uid-recreated")
			}
			p := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-new", "node-a", uid, corev1.PodRunning, now.Add(time.Second), "100Mi", true, 0)
			return true, &corev1.PodList{Items: []corev1.Pod{*p}}, nil
		})

		_, _, err := ensureCSIDriverMemoryLimit(ctx, client, "gcs-fuse-csi-driver", "gcsfusecsi-node", "node-a", oldPod, "100Mi", resource.MustParse("100Mi"))
		if err == nil || !strings.Contains(err.Error(), "changed UID") {
			t.Fatalf("expected UID change error during post-settle verification after pod rollover, got: %v", err)
		}
		if podListCalls.Load() != 2 {
			t.Errorf("got %d pod list calls, want 2 (rollout poll + post-settle refresh)", podListCalls.Load())
		}
	})

	t.Run("retries transient K8s API errors during post-settle pod refresh", func(t *testing.T) {
		oldPod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-old", "node-a", "uid-old", corev1.PodRunning, now, "256Mi", true, 0)
		ds := makeTestCSIDaemonSet("gcs-fuse-csi-driver", "gcsfusecsi-node", 1, "256Mi", "64Mi", "")
		client := fake.NewSimpleClientset(oldPod, ds)

		client.PrependReactor("update", "daemonsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
			upd := action.(k8stesting.UpdateAction).GetObject().(*appsv1.DaemonSet)
			upd.Generation++
			upd.Status.ObservedGeneration = upd.Generation
			upd.Status.DesiredNumberScheduled = 1
			upd.Status.UpdatedNumberScheduled = 1
			upd.Status.NumberAvailable = 1
			upd.Status.NumberUnavailable = 0
			return false, nil, nil
		})

		var podListCalls atomic.Int32
		client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			call := podListCalls.Add(1)
			switch call {
			case 2:
				return true, nil, &apierrors.StatusError{
					ErrStatus: metav1.Status{
						Code:    http.StatusBadGateway,
						Message: "bad gateway from apiserver proxy",
					},
				}
			case 3:
				return true, nil, apierrors.NewServiceUnavailable("apiserver temporarily unavailable")
			default:
				p := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-new", "node-a", "uid-new", corev1.PodRunning, now.Add(time.Second), "100Mi", true, 0)
				return true, &corev1.PodList{Items: []corev1.Pod{*p}}, nil
			}
		})

		gotPod, gotLimit, err := ensureCSIDriverMemoryLimit(ctx, client, "gcs-fuse-csi-driver", "gcsfusecsi-node", "node-a", oldPod, "100Mi", resource.MustParse("100Mi"))
		if err != nil {
			t.Fatalf("expected post-settle refresh retry to succeed, got error: %v", err)
		}
		if gotPod.UID != "uid-new" || gotLimit != "100Mi" {
			t.Errorf("got (UID=%s, limit=%q), want (uid-new, 100Mi)", gotPod.UID, gotLimit)
		}
		if podListCalls.Load() != 4 {
			t.Errorf("got %d pod list calls, want 4 (1 rollout poll + 3 post-settle refresh attempts)", podListCalls.Load())
		}
	})
}

func TestSelectTargetNodeAndCSIPod(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	makeNode := func(name string) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: map[string]string{"kubernetes.io/os": "linux"},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		}
	}

	t.Run("skips Failed/Succeeded evicted pods and prefers Running then newest CreationTimestamp", func(t *testing.T) {
		t.Parallel()
		// node-a only has Failed & Succeeded pods, so node-a must not be chosen.
		evictedOnA := makeTestCSIPod("gcs-fuse-csi-driver", "csi-evicted-a", "node-a", "u1", corev1.PodFailed, base.Add(10*time.Minute), "100Mi", false, 0)
		succeededOnA := makeTestCSIPod("gcs-fuse-csi-driver", "csi-done-a", "node-a", "u2", corev1.PodSucceeded, base.Add(11*time.Minute), "100Mi", false, 0)

		// node-b has an evicted PodFailed, a newer PodPending, an older PodRunning, and a newer PodRunning.
		evictedOnB := makeTestCSIPod("gcs-fuse-csi-driver", "csi-evicted-b", "node-b", "u3", corev1.PodFailed, base.Add(20*time.Minute), "100Mi", false, 0)
		pendingOnB := makeTestCSIPod("gcs-fuse-csi-driver", "csi-pending-b", "node-b", "u4", corev1.PodPending, base.Add(15*time.Minute), "100Mi", false, 0)
		olderRunningOnB := makeTestCSIPod("gcs-fuse-csi-driver", "csi-running-old", "node-b", "u5", corev1.PodRunning, base.Add(1*time.Minute), "100Mi", true, 0)
		newerRunningOnB := makeTestCSIPod("gcs-fuse-csi-driver", "csi-running-new", "node-b", "u6", corev1.PodRunning, base.Add(5*time.Minute), "100Mi", true, 0)

		client := fake.NewSimpleClientset(
			makeNode("node-a"),
			makeNode("node-b"),
			evictedOnA, succeededOnA, evictedOnB, pendingOnB, olderRunningOnB, newerRunningOnB,
		)

		gotNode, gotPod, err := selectTargetNodeAndCSIPod(ctx, client)
		if err != nil {
			t.Fatalf("selectTargetNodeAndCSIPod failed: %v", err)
		}
		if gotNode != "node-b" {
			t.Errorf("got node %q, want node-b", gotNode)
		}
		if gotPod.Name != "csi-running-new" {
			t.Errorf("got pod %q, want csi-running-new", gotPod.Name)
		}
	})

	t.Run("uses deterministic lexicographical name tie-breaker for equal timestamps", func(t *testing.T) {
		t.Parallel()
		podZ := makeTestCSIPod("gcs-fuse-csi-driver", "csi-z", "node-a", "uz", corev1.PodRunning, base, "100Mi", true, 0)
		podA := makeTestCSIPod("gcs-fuse-csi-driver", "csi-a", "node-a", "ua", corev1.PodRunning, base, "100Mi", true, 0)
		client := fake.NewSimpleClientset(makeNode("node-a"), podZ, podA)

		_, gotPod, err := selectTargetNodeAndCSIPod(ctx, client)
		if err != nil {
			t.Fatalf("selectTargetNodeAndCSIPod failed: %v", err)
		}
		if gotPod.Name != "csi-a" {
			t.Errorf("got pod %q, want csi-a", gotPod.Name)
		}
	})
}

func TestBuildSoakPodTemplateSpecAndDeploySoakWorkloads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := fake.NewSimpleClientset()
	const (
		ns         = "soak-ns"
		targetNode = "gke-node-1"
		bucketName = "soak-test-bucket"
	)
	mountOpts := buildMemorySoakMountOptions("grpc")

	if err := deploySoakWorkloads(ctx, client, ns, targetNode, bucketName, mountOpts, 60, 10, 10); err != nil {
		t.Fatalf("deploySoakWorkloads failed: %v", err)
	}

	verifyPodTemplate := func(t *testing.T, tmpl corev1.PodTemplateSpec, wantRestart corev1.RestartPolicy) {
		t.Helper()
		if tmpl.Labels[memorySoakWorkloadLabelKey] != memorySoakWorkloadLabelValue {
			t.Errorf("missing soak workload label on template: %+v", tmpl.Labels)
		}
		for k, want := range map[string]string{
			"gke-gcsfuse/volumes":                   "true",
			"gke-gcsfuse/ephemeral-storage-request": "200Mi",
			"gke-gcsfuse/ephemeral-storage-limit":   "200Mi",
		} {
			if got := tmpl.Annotations[k]; got != want {
				t.Errorf("annotation %q = %q, want %q", k, got, want)
			}
		}
		if tmpl.Spec.ServiceAccountName != specs.K8sServiceAccountName {
			t.Errorf("ServiceAccountName = %q, want %q", tmpl.Spec.ServiceAccountName, specs.K8sServiceAccountName)
		}
		if tmpl.Spec.AutomountServiceAccountToken == nil || *tmpl.Spec.AutomountServiceAccountToken {
			t.Errorf("AutomountServiceAccountToken = %v, want false", tmpl.Spec.AutomountServiceAccountToken)
		}
		if tmpl.Spec.RestartPolicy != wantRestart {
			t.Errorf("RestartPolicy = %q, want %q", tmpl.Spec.RestartPolicy, wantRestart)
		}
		if got := tmpl.Spec.NodeSelector["kubernetes.io/hostname"]; got != targetNode {
			t.Errorf("NodeSelector[kubernetes.io/hostname] = %q, want %q", got, targetNode)
		}
		if got := tmpl.Spec.NodeSelector["kubernetes.io/os"]; got != "linux" {
			t.Errorf("NodeSelector[kubernetes.io/os] = %q, want linux", got)
		}
		if len(tmpl.Spec.Volumes) != 1 || tmpl.Spec.Volumes[0].CSI == nil {
			t.Fatalf("expected 1 CSI volume, got %+v", tmpl.Spec.Volumes)
		}
		csiVol := tmpl.Spec.Volumes[0].CSI
		if csiVol.Driver != csidriver.DefaultName {
			t.Errorf("CSI driver = %q, want %q", csiVol.Driver, csidriver.DefaultName)
		}
		wantAttrs := map[string]string{
			csidriver.VolumeContextKeyBucketName:                bucketName,
			csidriver.VolumeContextKeyMountOptions:              mountOpts,
			csidriver.VolumeContextKeyDisableMetrics:            "false",
			csidriver.VolumeContextKeyFileCacheCapacity:         "20Mi",
			csidriver.VolumeContextKeyFileCacheForRangeRead:     "true",
			csidriver.VolumeContextKeyMetadataStatCacheCapacity: "4Mi",
			csidriver.VolumeContextKeyMetadataTypeCacheCapacity: "4Mi",
			csidriver.VolumeContextKeyMetadataCacheTTLSeconds:   "60",
		}
		if diff := cmp.Diff(wantAttrs, csiVol.VolumeAttributes); diff != "" {
			t.Errorf("CSI VolumeAttributes mismatch (-want +got):\n%s", diff)
		}
		script := tmpl.Spec.Containers[0].Args[0]
		loopIdx := strings.Index(script, "while true; do")
		mkdirIdx := strings.Index(script, `[ -d "${POD_DIR}" ] || mkdir -p "${POD_DIR}"`)
		if loopIdx < 0 || mkdirIdx < 0 || mkdirIdx < loopIdx {
			t.Errorf("expected guarded mkdir inside while loop in IO script; loopIdx=%d mkdirIdx=%d", loopIdx, mkdirIdx)
		}
	}

	deploy, err := client.AppsV1().Deployments(ns).Get(ctx, memorySoakDeployName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Deployment missing: %v", err)
	}
	if deploy.Spec.Replicas == nil || *deploy.Spec.Replicas != 60 {
		t.Errorf("Deployment replicas = %v, want 60", deploy.Spec.Replicas)
	}
	verifyPodTemplate(t, deploy.Spec.Template, corev1.RestartPolicyAlways)

	svc, err := client.CoreV1().Services(ns).Get(ctx, memorySoakStsSvcName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("headless Service missing: %v", err)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("Service ClusterIP = %q, want %q", svc.Spec.ClusterIP, corev1.ClusterIPNone)
	}

	sts, err := client.AppsV1().StatefulSets(ns).Get(ctx, memorySoakStsName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("StatefulSet missing: %v", err)
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 10 {
		t.Errorf("StatefulSet replicas = %v, want 10", sts.Spec.Replicas)
	}
	if sts.Spec.ServiceName != memorySoakStsSvcName {
		t.Errorf("StatefulSet ServiceName = %q, want %q", sts.Spec.ServiceName, memorySoakStsSvcName)
	}
	if sts.Spec.PodManagementPolicy != appsv1.ParallelPodManagement {
		t.Errorf("StatefulSet PodManagementPolicy = %q, want %q", sts.Spec.PodManagementPolicy, appsv1.ParallelPodManagement)
	}
	verifyPodTemplate(t, sts.Spec.Template, corev1.RestartPolicyAlways)

	job, err := client.BatchV1().Jobs(ns).Get(ctx, memorySoakJobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Job missing: %v", err)
	}
	if job.Spec.Parallelism == nil || *job.Spec.Parallelism != 10 || job.Spec.Completions == nil || *job.Spec.Completions != 10 {
		t.Errorf("Job parallelism/completions = (%v, %v), want (10, 10)", job.Spec.Parallelism, job.Spec.Completions)
	}
	if got := job.Spec.Template.Labels[memorySoakJobSetLabel]; got != memorySoakJobName {
		t.Errorf("Job template label %q = %q, want %q", memorySoakJobSetLabel, got, memorySoakJobName)
	}
	verifyPodTemplate(t, job.Spec.Template, corev1.RestartPolicyOnFailure)
}

func TestWriteSoakDiagnostics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("falls back to summary.CSIPodName when findActiveCSIPodOnNode fails and writes failed summary JSON", func(t *testing.T) {
		t.Parallel()
		client := fake.NewSimpleClientset() // No pods exist -> findActiveCSIPodOnNode returns error.
		artifactsDir := t.TempDir()
		summary := &memorySoakSummary{
			Status:       "failed",
			CSINamespace: "gcs-fuse-csi-driver",
			TargetNode:   "node-a",
			CSIPodName:   "gcsfusecsi-node-fallback",
		}

		writeSoakDiagnostics(ctx, client, artifactsDir, summary)

		summaryBytes, err := os.ReadFile(filepath.Join(artifactsDir, "memory_soak_summary.json"))
		if err != nil {
			t.Fatalf("failed to read memory_soak_summary.json: %v", err)
		}
		var parsed memorySoakSummary
		if err := json.Unmarshal(summaryBytes, &parsed); err != nil {
			t.Fatalf("failed to unmarshal memory_soak_summary.json: %v", err)
		}
		if parsed.Status != "failed" {
			t.Errorf("summary.Status = %q, want failed", parsed.Status)
		}
		if parsed.EndTime == "" {
			t.Error("expected summary.EndTime to be populated")
		}
		if len(parsed.DiagnosticWriteErrors) == 0 || !strings.Contains(parsed.DiagnosticWriteErrors[0], "fetch final CSI pod") {
			t.Errorf("expected fetch final CSI pod error recorded, got %+v", parsed.DiagnosticWriteErrors)
		}

		logBytes, err := os.ReadFile(filepath.Join(artifactsDir, "csi_driver_logs.txt"))
		if err != nil {
			t.Fatalf("expected csi_driver_logs.txt via fallback pod name, got error: %v", err)
		}
		logText := string(logBytes)
		if !strings.Contains(logText, "=== CURRENT LOGS (gcs-fuse-csi-driver/gcsfusecsi-node-fallback") {
			t.Errorf("missing CURRENT LOGS header for fallback pod in %q", logText)
		}
		if !strings.Contains(logText, "=== PREVIOUS LOGS (gcs-fuse-csi-driver/gcsfusecsi-node-fallback") {
			t.Errorf("missing PREVIOUS LOGS header for failed fallback pod in %q", logText)
		}
	})

	t.Run("captures FinalCSIContainer and skips previous logs when passed with 0 restarts", func(t *testing.T) {
		t.Parallel()
		now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
		pod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-live", "node-a", "uid-live", corev1.PodRunning, now, "100Mi", true, 0)
		client := fake.NewSimpleClientset(pod)
		artifactsDir := t.TempDir()
		summary := &memorySoakSummary{
			Status:       "passed",
			CSINamespace: "gcs-fuse-csi-driver",
			TargetNode:   "node-a",
			CSIPodName:   "gcsfusecsi-node-live",
		}

		writeSoakDiagnostics(ctx, client, artifactsDir, summary)

		summaryBytes, err := os.ReadFile(filepath.Join(artifactsDir, "memory_soak_summary.json"))
		if err != nil {
			t.Fatalf("failed to read memory_soak_summary.json: %v", err)
		}
		var parsed memorySoakSummary
		if err := json.Unmarshal(summaryBytes, &parsed); err != nil {
			t.Fatalf("failed to unmarshal memory_soak_summary.json: %v", err)
		}
		if parsed.Status != "passed" {
			t.Errorf("summary.Status = %q, want passed", parsed.Status)
		}
		if parsed.FinalCSIContainer == nil || !parsed.FinalCSIContainer.Ready || !parsed.FinalCSIContainer.Running || parsed.FinalCSIContainer.RestartCount != 0 {
			t.Errorf("unexpected FinalCSIContainer: %+v", parsed.FinalCSIContainer)
		}

		logBytes, err := os.ReadFile(filepath.Join(artifactsDir, "csi_driver_logs.txt"))
		if err != nil {
			t.Fatalf("failed to read csi_driver_logs.txt: %v", err)
		}
		logText := string(logBytes)
		if !strings.Contains(logText, "=== CURRENT LOGS (gcs-fuse-csi-driver/gcsfusecsi-node-live") {
			t.Errorf("missing CURRENT LOGS header in %q", logText)
		}
		if strings.Contains(logText, "=== PREVIOUS LOGS") {
			t.Errorf("unexpected PREVIOUS LOGS header when passed with 0 restarts: %q", logText)
		}
	})

	t.Run("writes diagnostic artifacts with non-owner read permissions modulo process umask", func(t *testing.T) {
		t.Parallel()
		now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
		pod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-perm", "node-a", "uid-perm", corev1.PodRunning, now, "100Mi", true, 0)
		client := fake.NewSimpleClientset(pod)
		artifactsDir := t.TempDir()
		summary := &memorySoakSummary{
			Status:       "passed",
			CSINamespace: "gcs-fuse-csi-driver",
			TargetNode:   "node-a",
			CSIPodName:   "gcsfusecsi-node-perm",
		}

		writeSoakDiagnostics(ctx, client, artifactsDir, summary)

		probePath := filepath.Join(t.TempDir(), "umask-probe")
		probeFile, err := os.Create(probePath)
		if err != nil {
			t.Fatalf("failed to create umask probe file: %v", err)
		}
		if err := probeFile.Close(); err != nil {
			t.Fatalf("failed to close umask probe file: %v", err)
		}
		probeInfo, err := os.Stat(probePath)
		if err != nil {
			t.Fatalf("failed to stat umask probe file: %v", err)
		}
		expectedReadMask := probeInfo.Mode().Perm() & 0o044

		for _, name := range []string{"csi_driver_logs.txt", "memory_soak_summary.json"} {
			info, err := os.Stat(filepath.Join(artifactsDir, name))
			if err != nil {
				t.Fatalf("failed to stat %s: %v", name, err)
			}
			perm := info.Mode().Perm()
			if perm&0o600 != 0o600 {
				t.Errorf("%s permissions = %04o, want owner read/write bits (0600) set", name, perm)
			}
			if perm&expectedReadMask != expectedReadMask {
				t.Errorf("%s permissions = %04o, want non-owner read bits %04o preserved (probe=%04o)", name, perm, expectedReadMask, probeInfo.Mode().Perm())
			}
			if perm&0o133 != 0 {
				t.Errorf("%s permissions = %04o, want no execute or non-owner write bits set", name, perm)
			}
		}
	})
}

func TestScrapeAndAnalyzeCSIMetrics(t *testing.T) {
	origFirst := memorySoakFirstScrapeRetryDelay
	origSteady := memorySoakSteadyScrapeRetryDelay
	memorySoakFirstScrapeRetryDelay = time.Millisecond
	memorySoakSteadyScrapeRetryDelay = time.Millisecond
	t.Cleanup(func() {
		memorySoakFirstScrapeRetryDelay = origFirst
		memorySoakSteadyScrapeRetryDelay = origSteady
	})

	ctx := context.Background()
	expectedPods := map[string]struct{}{
		"pod-1": {},
		"pod-2": {},
	}
	fullPayload := []byte(strings.Join([]string{
		`volume_fs_ops_count{namespace_name="soak-ns",pod_name="pod-1"} 1`,
		`volume_fs_ops_count{namespace_name="soak-ns",pod_name="pod-2"} 2`,
	}, "\n"))
	partialPayload := []byte(`volume_fs_ops_count{namespace_name="soak-ns",pod_name="pod-1"} 1`)

	t.Run("steady-state scrape recovers from transient error on retry", func(t *testing.T) {
		var attempts int
		scrapeFn := func(_ context.Context) ([]byte, error) {
			attempts++
			if attempts == 1 {
				return nil, errors.New("connection reset by peer")
			}
			return fullPayload, nil
		}
		healthChecks := 0
		healthCheck := func(_ context.Context) error {
			healthChecks++
			return nil
		}

		body, _, analysis, err := scrapeAndAnalyzeCSIMetricsWithFunc(ctx, scrapeFn, "soak-ns", expectedPods, false, healthCheck)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if attempts != 2 {
			t.Errorf("got %d attempts, want 2", attempts)
		}
		if healthChecks != 1 {
			t.Errorf("got %d health checks, want 1", healthChecks)
		}
		if len(body) == 0 || analysis.DistinctPods != 2 {
			t.Errorf("unexpected scrape result: bytes=%d analysis=%+v", len(body), analysis)
		}
	})

	t.Run("resets lastAnalysis when subsequent retry fails with transport error", func(t *testing.T) {
		var attempts int
		scrapeFn := func(_ context.Context) ([]byte, error) {
			attempts++
			if attempts == 1 {
				return partialPayload, nil
			}
			return nil, errors.New("EOF")
		}

		body, _, analysis, err := scrapeAndAnalyzeCSIMetricsWithFunc(ctx, scrapeFn, "soak-ns", expectedPods, false, func(_ context.Context) error {
			return nil
		})
		if err == nil {
			t.Fatal("expected transport error, got nil")
		}
		if attempts != memorySoakSteadyScrapeMaxAttempts {
			t.Errorf("got %d attempts, want %d", attempts, memorySoakSteadyScrapeMaxAttempts)
		}
		if len(body) != 0 {
			t.Errorf("expected empty body on transport failure, got %d bytes", len(body))
		}
		if analysis.Bytes != 0 || analysis.SeriesCount != 0 || analysis.DistinctPods != 0 || len(analysis.MissingPods) != 0 {
			t.Errorf("expected zeroed lastAnalysis after transport error (no stale attempt 1 data), got %+v", analysis)
		}
	})

	t.Run("aborts immediately without retrying when CSI driver health check detects OOMKill", func(t *testing.T) {
		var attempts int
		scrapeFn := func(_ context.Context) ([]byte, error) {
			attempts++
			return nil, errors.New("dial tcp: connection refused")
		}
		healthCheck := func(_ context.Context) error {
			return errors.New("container gcs-fuse-csi-driver was OOMKilled (exitCode=137)")
		}

		_, _, _, err := scrapeAndAnalyzeCSIMetricsWithFunc(ctx, scrapeFn, "soak-ns", expectedPods, false, healthCheck)
		if err == nil {
			t.Fatal("expected error on unhealthy CSI driver, got nil")
		}
		if attempts != 1 {
			t.Errorf("expected immediate abort after 1 attempt, got %d attempts", attempts)
		}
		if !strings.Contains(err.Error(), "OOMKilled") {
			t.Errorf("expected error to mention OOMKilled, got: %v", err)
		}
	})

	t.Run("first scrape retries up to memorySoakFirstScrapeMaxAttempts", func(t *testing.T) {
		var attempts int
		scrapeFn := func(_ context.Context) ([]byte, error) {
			attempts++
			if attempts < memorySoakFirstScrapeMaxAttempts {
				return partialPayload, nil
			}
			return fullPayload, nil
		}

		_, _, analysis, err := scrapeAndAnalyzeCSIMetricsWithFunc(ctx, scrapeFn, "soak-ns", expectedPods, true, func(_ context.Context) error {
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if attempts != memorySoakFirstScrapeMaxAttempts {
			t.Errorf("got %d attempts, want %d", attempts, memorySoakFirstScrapeMaxAttempts)
		}
		if analysis.DistinctPods != 2 {
			t.Errorf("got DistinctPods=%d, want 2", analysis.DistinctPods)
		}
	})

	t.Run("returns ctx.Err() without invoking scrapeFn or healthCheck when parent ctx is pre-canceled", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		var scrapeCalls, healthCalls int
		scrapeFn := func(_ context.Context) ([]byte, error) {
			scrapeCalls++
			return nil, errors.New("should not be called")
		}
		healthCheck := func(_ context.Context) error {
			healthCalls++
			return errors.New("should not be called")
		}

		_, _, _, err := scrapeAndAnalyzeCSIMetricsWithFunc(canceledCtx, scrapeFn, "soak-ns", expectedPods, false, healthCheck)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got error %v, want context.Canceled", err)
		}
		if strings.Contains(err.Error(), "CSI driver unhealthy") {
			t.Errorf("error %q must not misattribute cancellation to CSI driver unhealthy", err.Error())
		}
		if scrapeCalls != 0 || healthCalls != 0 {
			t.Errorf("got scrapeCalls=%d healthCalls=%d, want 0 and 0", scrapeCalls, healthCalls)
		}
	})

	t.Run("returns ctx.Err() without invoking healthCheck when parent ctx is canceled mid-scrape", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(ctx)
		var scrapeCalls, healthCalls int
		scrapeFn := func(scrapeCtx context.Context) ([]byte, error) {
			scrapeCalls++
			cancel()
			return nil, scrapeCtx.Err()
		}
		healthCheck := func(_ context.Context) error {
			healthCalls++
			return errors.New("should not be called")
		}

		_, _, _, err := scrapeAndAnalyzeCSIMetricsWithFunc(parentCtx, scrapeFn, "soak-ns", expectedPods, false, healthCheck)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got error %v, want context.Canceled", err)
		}
		if strings.Contains(err.Error(), "CSI driver unhealthy") {
			t.Errorf("error %q must not misattribute cancellation to CSI driver unhealthy", err.Error())
		}
		if scrapeCalls != 1 || healthCalls != 0 {
			t.Errorf("got scrapeCalls=%d healthCalls=%d, want 1 and 0", scrapeCalls, healthCalls)
		}
	})

	t.Run("returns ctx.Err() without invoking healthCheck when parent ctx expires mid-scrape", func(t *testing.T) {
		parentCtx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
		defer cancel()

		var scrapeCalls, healthCalls int
		scrapeFn := func(scrapeCtx context.Context) ([]byte, error) {
			scrapeCalls++
			<-parentCtx.Done()
			return nil, scrapeCtx.Err()
		}
		healthCheck := func(_ context.Context) error {
			healthCalls++
			return errors.New("should not be called")
		}

		_, _, _, err := scrapeAndAnalyzeCSIMetricsWithFunc(parentCtx, scrapeFn, "soak-ns", expectedPods, false, healthCheck)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got error %v, want context.DeadlineExceeded", err)
		}
		if strings.Contains(err.Error(), "CSI driver unhealthy") {
			t.Errorf("error %q must not misattribute deadline expiry to CSI driver unhealthy", err.Error())
		}
		if scrapeCalls != 1 || healthCalls != 0 {
			t.Errorf("got scrapeCalls=%d healthCalls=%d, want 1 and 0", scrapeCalls, healthCalls)
		}
	})

	t.Run("returns ctx.Err() instead of CSI driver unhealthy when parent ctx expires during healthCheck", func(t *testing.T) {
		parentCtx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
		defer cancel()

		var scrapeCalls, healthCalls int
		scrapeFn := func(_ context.Context) ([]byte, error) {
			scrapeCalls++
			return nil, errors.New("EOF")
		}
		healthCheck := func(hcCtx context.Context) error {
			healthCalls++
			<-hcCtx.Done()
			return hcCtx.Err()
		}

		_, _, _, err := scrapeAndAnalyzeCSIMetricsWithFunc(parentCtx, scrapeFn, "soak-ns", expectedPods, false, healthCheck)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got error %v, want context.DeadlineExceeded", err)
		}
		if strings.Contains(err.Error(), "CSI driver unhealthy") {
			t.Errorf("error %q must not misattribute deadline expiry to CSI driver unhealthy", err.Error())
		}
		if scrapeCalls != 1 || healthCalls != 1 {
			t.Errorf("got scrapeCalls=%d healthCalls=%d, want 1 and 1", scrapeCalls, healthCalls)
		}
	})
}

func TestCheckCSIPodHealthBeforeScrapeRetry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	t.Run("returns hcCtx.Err() immediately without API call when parent hcCtx is already expired", func(t *testing.T) {
		t.Parallel()
		expiredCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		client := fake.NewSimpleClientset()
		err := checkCSIPodHealthBeforeScrapeRetry(expiredCtx, client, "gcs-fuse-csi-driver", "node-a", "uid-1", 0, 1)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got error %v, want context.DeadlineExceeded", err)
		}
		if len(client.Actions()) != 0 {
			t.Errorf("expected no K8s API actions when hcCtx is already expired, got %d", len(client.Actions()))
		}
	})

	t.Run("does not swallow DeadlineExceeded when parent hcCtx expires during API call", func(t *testing.T) {
		t.Parallel()
		parentCtx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
		defer cancel()

		client := fake.NewSimpleClientset()
		client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			<-parentCtx.Done()
			return true, nil, parentCtx.Err()
		})

		err := checkCSIPodHealthBeforeScrapeRetry(parentCtx, client, "gcs-fuse-csi-driver", "node-a", "uid-1", 0, 1)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got error %v, want context.DeadlineExceeded (must not swallow expired parent hcCtx)", err)
		}
	})

	t.Run("swallows per-call DeadlineExceeded when parent hcCtx is still live", func(t *testing.T) {
		t.Parallel()
		client := fake.NewSimpleClientset()
		client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, context.DeadlineExceeded
		})

		if err := checkCSIPodHealthBeforeScrapeRetry(context.Background(), client, "gcs-fuse-csi-driver", "node-a", "uid-1", 0, 1); err != nil {
			t.Fatalf("expected nil on per-call timeout with live parent hcCtx, got: %v", err)
		}
	})

	t.Run("swallows transient HTTP 502 error when parent hcCtx is still live", func(t *testing.T) {
		t.Parallel()
		client := fake.NewSimpleClientset()
		client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, &apierrors.StatusError{
				ErrStatus: metav1.Status{
					Code:    http.StatusBadGateway,
					Message: "bad gateway",
				},
			}
		})

		if err := checkCSIPodHealthBeforeScrapeRetry(context.Background(), client, "gcs-fuse-csi-driver", "node-a", "uid-1", 0, 1); err != nil {
			t.Fatalf("expected nil on transient HTTP 502 with live parent hcCtx, got: %v", err)
		}
	})

	t.Run("returns container health error when CSI pod OOMKilled on live hcCtx", func(t *testing.T) {
		t.Parallel()
		pod := makeTestCSIPod("gcs-fuse-csi-driver", "gcsfusecsi-node-1", "node-a", "uid-1", corev1.PodRunning, now, "100Mi", true, 1)
		pod.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137},
		}
		client := fake.NewSimpleClientset(pod)

		err := checkCSIPodHealthBeforeScrapeRetry(context.Background(), client, "gcs-fuse-csi-driver", "node-a", "uid-1", 0, 1)
		if err == nil || !strings.Contains(err.Error(), "OOMKilled") {
			t.Fatalf("expected OOMKilled error, got: %v", err)
		}
	})
}

func TestIsTransientK8sAPIError(t *testing.T) {
	t.Parallel()

	statusErr := func(code int32, msg string) error {
		return &apierrors.StatusError{
			ErrStatus: metav1.Status{
				Status:  metav1.StatusFailure,
				Code:    code,
				Message: msg,
			},
		}
	}

	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "HTTP 429 TooManyRequests",
			err:  apierrors.NewTooManyRequests("rate limited", 1),
			want: true,
		},
		{
			name: "HTTP 500 InternalServerError",
			err:  apierrors.NewInternalError(errors.New("internal")),
			want: true,
		},
		{
			name: "HTTP 502 BadGateway structured APIStatus",
			err:  statusErr(http.StatusBadGateway, "bad gateway"),
			want: true,
		},
		{
			name: "wrapped HTTP 502 BadGateway structured APIStatus",
			err:  fmt.Errorf("apiserver proxy error: %w", statusErr(http.StatusBadGateway, "bad gateway")),
			want: true,
		},
		{
			name: "HTTP 503 ServiceUnavailable",
			err:  apierrors.NewServiceUnavailable("unavailable"),
			want: true,
		},
		{
			name: "HTTP 504 GatewayTimeout structured APIStatus",
			err:  statusErr(http.StatusGatewayTimeout, "gateway timeout"),
			want: true,
		},
		{
			name: "context.DeadlineExceeded",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "HTTP 403 Forbidden",
			err:  apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "pod-1", errors.New("forbidden")),
			want: false,
		},
		{
			name: "HTTP 404 NotFound",
			err:  apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "pod-1"),
			want: false,
		},
		{
			name: "HTTP 501 NotImplemented structured APIStatus",
			err:  statusErr(http.StatusNotImplemented, "not implemented"),
			want: false,
		},
		{
			name: "context.Canceled",
			err:  context.Canceled,
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isTransientK8sAPIError(tc.err); got != tc.want {
				t.Errorf("isTransientK8sAPIError(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestRetryTransientK8sAPICall(t *testing.T) {
	origDelay := memorySoakAPIRetryDelay
	memorySoakAPIRetryDelay = time.Millisecond
	t.Cleanup(func() {
		memorySoakAPIRetryDelay = origDelay
	})

	ctx := context.Background()

	t.Run("retries transient API server error and succeeds", func(t *testing.T) {
		var attempts int
		got, err := retryTransientK8sAPICall(ctx, "test-op", func(_ context.Context) (string, error) {
			attempts++
			if attempts < 3 {
				return "", apierrors.NewServiceUnavailable("apiserver temporarily unavailable")
			}
			return "ok", nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "ok" || attempts != 3 {
			t.Errorf("got (%q, attempts=%d), want (ok, 3)", got, attempts)
		}
	})

	t.Run("retries structured HTTP 502 BadGateway and succeeds", func(t *testing.T) {
		var attempts int
		got, err := retryTransientK8sAPICall(ctx, "test-502", func(_ context.Context) (string, error) {
			attempts++
			if attempts == 1 {
				return "", &apierrors.StatusError{
					ErrStatus: metav1.Status{
						Code:    http.StatusBadGateway,
						Message: "bad gateway",
					},
				}
			}
			return "recovered", nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "recovered" || attempts != 2 {
			t.Errorf("got (%q, attempts=%d), want (recovered, 2)", got, attempts)
		}
	})

	t.Run("stops immediately when parent ctx expires mid-call", func(t *testing.T) {
		parentCtx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
		defer cancel()

		var attempts int
		_, err := retryTransientK8sAPICall(parentCtx, "test-parent-expiry", func(callCtx context.Context) (string, error) {
			attempts++
			<-parentCtx.Done()
			return "", callCtx.Err()
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got error %v, want context.DeadlineExceeded", err)
		}
		if attempts != 1 {
			t.Errorf("expected 1 attempt when parent ctx expires, got %d", attempts)
		}
	})

	t.Run("stops immediately without calling fn when parent ctx is already expired", func(t *testing.T) {
		expiredCtx, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer cancel()

		var attempts int
		_, err := retryTransientK8sAPICall(expiredCtx, "test-pre-expired", func(_ context.Context) (string, error) {
			attempts++
			return "unreachable", nil
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got error %v, want context.DeadlineExceeded", err)
		}
		if attempts != 0 {
			t.Errorf("expected 0 attempts when parent ctx already expired, got %d", attempts)
		}
	})

	t.Run("does not retry non-transient error", func(t *testing.T) {
		var attempts int
		_, err := retryTransientK8sAPICall(ctx, "test-op", func(_ context.Context) (string, error) {
			attempts++
			return "", errors.New("no active CSI driver pod found")
		})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if attempts != 1 {
			t.Errorf("expected 1 attempt for non-transient error, got %d", attempts)
		}
	})
}
