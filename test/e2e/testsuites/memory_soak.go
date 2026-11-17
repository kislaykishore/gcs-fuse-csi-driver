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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"local/test/e2e/specs"

	csidriver "github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/csi_driver"
	"github.com/onsi/ginkgo/v2"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/test/e2e/framework"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"
)

const (
	memorySoakCSIAppLabel          = "k8s-app=gcs-fuse-csi-driver"
	memorySoakCSIContainerName     = "gcs-fuse-csi-driver"
	memorySoakCSIDaemonSetFallback = "gcsfusecsi-node"
	memorySoakManagedNamespace     = "kube-system"

	memorySoakWorkloadLabelKey   = "gcsfuse-soak-workload"
	memorySoakWorkloadLabelValue = "true"
	memorySoakWorkloadSelector   = memorySoakWorkloadLabelKey + "=" + memorySoakWorkloadLabelValue

	memorySoakDeployName  = "gcsfuse-soak-deploy"
	memorySoakStsName     = "gcsfuse-soak-sts"
	memorySoakStsSvcName  = "gcsfuse-soak-sts-svc"
	memorySoakJobName     = "gcsfuse-soak-jobset"
	memorySoakJobSetLabel = "jobset.sigs.k8s.io/jobset-name"
	memorySoakVolumeName  = "gcsfuse-vol"
	memorySoakMountPath   = "/data"

	memorySoakSeedFileCount = 5
	memorySoakSeedFileBytes = 1 << 20 // 1 MiB

	memorySoakRolloutTimeout  = 5 * time.Minute
	memorySoakPodReadyTimeout = 15 * time.Minute
	memorySoakCleanupTimeout  = 3 * time.Minute
	memorySoakDefaultWarmup   = 2 * time.Minute
	memorySoakScrapeTimeout   = 60 * time.Second
	memorySoakDiagTimeout     = 30 * time.Second
	memorySoakLogTailLines    = int64(2000)

	memorySoakFirstScrapeMaxAttempts  = 4
	memorySoakSteadyScrapeMaxAttempts = 3
	memorySoakAPIMaxAttempts          = 3

	memorySoakIOScript = `set -u
MOUNT="/data"
POD_DIR="${MOUNT}/pods/${POD_NAME}"
i=0
while true; do
  i=$((i + 1))
  [ -d "${POD_DIR}" ] || mkdir -p "${POD_DIR}" 2>/dev/null || true
  # 1. Sequential read from shared seed files (first pass misses FileCache -> GCS read; subsequent passes hit FileCache)
  cat "${MOUNT}/perf-shared/seed_1.bin" "${MOUNT}/perf-shared/seed_2.bin" > /dev/null 2>&1 || true
  # 2. Random offset range reads using dd skip (triggers random FileCache read_type & range reads)
  dd if="${MOUNT}/perf-shared/seed_3.bin" of=/dev/null bs=4096 skip=10 count=4 2>/dev/null || true
  dd if="${MOUNT}/perf-shared/seed_4.bin" of=/dev/null bs=4096 skip=35 count=4 2>/dev/null || true
  dd if="${MOUNT}/perf-shared/seed_5.bin" of=/dev/null bs=4096 skip=2 count=4 2>/dev/null || true
  # 3. Create, write, fsync, rename, read, unlink per-pod file
  dd if=/dev/zero of="${POD_DIR}/temp_write.bin" bs=32768 count=4 conv=fsync 2>/dev/null || dd if=/dev/zero of="${POD_DIR}/temp_write.bin" bs=32768 count=4 2>/dev/null || true
  mv "${POD_DIR}/temp_write.bin" "${POD_DIR}/committed.bin" 2>/dev/null || true
  cat "${POD_DIR}/committed.bin" > /dev/null 2>&1 || true
  # 4. Metadata listing & positive stat
  ls -la "${MOUNT}/perf-shared" "${POD_DIR}" > /dev/null 2>&1 || true
  # 5. Intentional ENOENT negative lookup to exercise fs_ops_error_count
  cat "${POD_DIR}/non_existent_${i}.txt" > /dev/null 2>&1 || true
  rm -f "${POD_DIR}/committed.bin" 2>/dev/null || true
  sleep 2
done
`
)

var (
	// MemorySoakDuration controls how long the soak loop runs after workload warmup.
	MemorySoakDuration = 4 * time.Hour
	// MemorySoakScrapeInterval controls how frequently the test actively scrapes :9920/metrics.
	MemorySoakScrapeInterval = 60 * time.Second
	// MemorySoakPodCount controls the total number of concurrent workload pods pinned to one node.
	MemorySoakPodCount = 80
	// MemorySoakCSIDriverMemoryLimit enforces the container memory limit on ds/gcsfusecsi-node
	// (gcs-fuse-csi-driver container) for non-managed driver installs. Empty means leave unchanged.
	MemorySoakCSIDriverMemoryLimit = "50Mi"

	// Package-level retry/settle durations so unit tests can override them without slow sleeps.
	memorySoakRolloutSettleDuration  = 10 * time.Second
	memorySoakRolloutPollInterval    = 3 * time.Second
	memorySoakFirstScrapeRetryDelay  = 10 * time.Second
	memorySoakSteadyScrapeRetryDelay = 5 * time.Second
	memorySoakAPIRetryDelay          = 2 * time.Second
)

type memorySoakScrapeAnalysis struct {
	Bytes        int
	SeriesCount  int
	DistinctPods int
	MissingPods  []string
}

type memorySoakSample struct {
	SampleIndex       int     `json:"sampleIndex"`
	Timestamp         string  `json:"timestamp"`
	ElapsedSeconds    float64 `json:"elapsedSeconds"`
	ScrapeDurationMs  int64   `json:"scrapeDurationMs"`
	ScrapeBytes       int     `json:"scrapeBytes"`
	ScrapeSeriesCount int     `json:"scrapeSeriesCount"`
	DistinctPods      int     `json:"distinctPods"`
	WorkingSetMiB     float64 `json:"workingSetMiB"`
	RSSMiB            float64 `json:"rssMiB"`
	CgroupPeakMiB     float64 `json:"cgroupPeakMiB"`
	CSIRestartCount   int32   `json:"csiRestartCount"`
}

type memorySoakCSIContainerSummary struct {
	Ready                bool   `json:"ready"`
	RestartCount         int32  `json:"restartCount"`
	Running              bool   `json:"running"`
	TerminatedReason     string `json:"terminatedReason,omitempty"`
	TerminatedExitCode   int32  `json:"terminatedExitCode,omitempty"`
	LastTerminatedReason string `json:"lastTerminatedReason,omitempty"`
	LastTerminatedExit   int32  `json:"lastTerminatedExitCode,omitempty"`
}

type memorySoakSummary struct {
	Status                string                         `json:"status"`
	StartTime             string                         `json:"startTime"`
	EndTime               string                         `json:"endTime"`
	DurationConfigured    string                         `json:"durationConfigured"`
	ScrapeInterval        string                         `json:"scrapeInterval"`
	PodCount              int                            `json:"podCount"`
	ClientProtocol        string                         `json:"clientProtocol"`
	RequestedMemoryLimit  string                         `json:"requestedMemoryLimit"`
	EffectiveMemoryLimit  string                         `json:"effectiveMemoryLimit"`
	TargetNode            string                         `json:"targetNode"`
	CSINamespace          string                         `json:"csiNamespace"`
	CSIDaemonSetName      string                         `json:"csiDaemonSetName"`
	CSIPodName            string                         `json:"csiPodName"`
	InitialPodUID         string                         `json:"initialPodUID"`
	InitialRestartCount   int32                          `json:"initialRestartCount"`
	TotalSamples          int                            `json:"totalSamples"`
	MaxWorkingSetMiB      float64                        `json:"maxWorkingSetMiB"`
	MaxRSSMiB             float64                        `json:"maxRSSMiB"`
	MaxCgroupPeakMiB      float64                        `json:"maxCgroupPeakMiB"`
	FinalCSIContainer     *memorySoakCSIContainerSummary `json:"finalCSIContainer,omitempty"`
	Samples               []memorySoakSample             `json:"samples"`
	DiagnosticWriteErrors []string                       `json:"diagnosticWriteErrors,omitempty"`
}

type kubeletStatsSummary struct {
	Pods []kubeletPodStats `json:"pods"`
}

type kubeletPodStats struct {
	PodRef struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"podRef"`
	Containers []kubeletContainerStats `json:"containers"`
}

type kubeletContainerStats struct {
	Name   string `json:"name"`
	Memory *struct {
		WorkingSetBytes *uint64 `json:"workingSetBytes"`
		RSSBytes        *uint64 `json:"rssBytes"`
	} `json:"memory"`
}

type gcsFuseCSIMemorySoakTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

// InitGcsFuseCSIMemorySoakTestSuite returns gcsFuseCSIMemorySoakTestSuite that implements TestSuite interface.
func InitGcsFuseCSIMemorySoakTestSuite() storageframework.TestSuite {
	return &gcsFuseCSIMemorySoakTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name: "memory-soak",
			TestPatterns: []storageframework.TestPattern{
				storageframework.DefaultFsCSIEphemeralVolume,
			},
		},
	}
}

func (t *gcsFuseCSIMemorySoakTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *gcsFuseCSIMemorySoakTestSuite) SkipUnsupportedTests(_ storageframework.TestDriver, _ storageframework.TestPattern) {
}

//nolint:maintidx
func (t *gcsFuseCSIMemorySoakTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	gcsfuseDriver, ok := driver.(*specs.GCSFuseCSITestDriver)
	if !ok {
		framework.Failf("This test requires a GCSFuseCSITestDriver but received a %T", driver)
	}

	// Beware that it also registers an AfterEach which renders f unusable. Any code using
	// f must run inside an It or Context callback.
	f := framework.NewFrameworkWithCustomTimeouts("memory-soak", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	ginkgo.It("should succeed in memory soak test", func(ctx ginkgo.SpecContext) {
		client := f.ClientSet
		namespace := f.Namespace.Name

		ginkgo.By("Validating memory soak test configuration")
		desiredLimit, err := validateMemorySoakConfig(
			MemorySoakDuration,
			MemorySoakScrapeInterval,
			MemorySoakPodCount,
			MemorySoakCSIDriverMemoryLimit,
		)
		framework.ExpectNoError(err, "invalid memory soak configuration")

		artifactsDir := "../../_artifacts"
		if dir, ok := os.LookupEnv("ARTIFACTS"); ok && dir != "" {
			artifactsDir = dir
		}

		summary := &memorySoakSummary{
			Status:               "failed",
			StartTime:            time.Now().UTC().Format(time.RFC3339),
			DurationConfigured:   MemorySoakDuration.String(),
			ScrapeInterval:       MemorySoakScrapeInterval.String(),
			PodCount:             MemorySoakPodCount,
			ClientProtocol:       gcsfuseDriver.ClientProtocol,
			RequestedMemoryLimit: MemorySoakCSIDriverMemoryLimit,
			Samples:              []memorySoakSample{},
		}

		var diagOnce sync.Once
		collectDiag := func(cleanupCtx context.Context) {
			diagOnce.Do(func() {
				writeSoakDiagnostics(cleanupCtx, client, artifactsDir, summary)
			})
		}
		// Register early via DeferCleanup so failures or spec cancellation/timeout during
		// rollout, baseline check, bucket prep, or seeding always emit
		// memory_soak_summary.json and csi_driver_logs.txt with a fresh cleanup SpecContext.
		ginkgo.DeferCleanup(func(cleanupCtx ginkgo.SpecContext) {
			collectDiag(cleanupCtx)
		}, ginkgo.NodeTimeout(memorySoakDiagTimeout+30*time.Second))

		ginkgo.By("Selecting a schedulable Linux node running the CSI node driver")
		targetNode, csiPod, err := selectTargetNodeAndCSIPod(ctx, client)
		framework.ExpectNoError(err, "failed to select target node and CSI driver pod")
		csiNamespace := csiPod.Namespace
		csiDaemonSetName := resolveCSIDaemonSetName(csiPod)
		summary.TargetNode = targetNode
		summary.CSINamespace = csiNamespace
		summary.CSIDaemonSetName = csiDaemonSetName
		summary.CSIPodName = csiPod.Name
		framework.Logf("Selected target node %q with CSI pod %s/%s (DaemonSet %q)", targetNode, csiNamespace, csiPod.Name, csiDaemonSetName)

		ginkgo.By("Enforcing or verifying CSI node driver memory limit")
		csiPod, effectiveLimit, err := ensureCSIDriverMemoryLimit(
			ctx,
			client,
			csiNamespace,
			csiDaemonSetName,
			targetNode,
			csiPod,
			MemorySoakCSIDriverMemoryLimit,
			desiredLimit,
		)
		framework.ExpectNoError(err, "failed to enforce/verify CSI node driver memory limit")
		summary.CSIPodName = csiPod.Name
		summary.EffectiveMemoryLimit = effectiveLimit

		ginkgo.By("Snapshotting baseline CSI node driver pod UID and restart count")
		initialPodUID := csiPod.UID
		initialStatus, err := getCSIContainerStatus(csiPod)
		framework.ExpectNoError(err, "failed to inspect baseline CSI driver container status")
		initialRestartCount := initialStatus.RestartCount
		framework.ExpectNoError(
			checkCSIContainerHealth(csiPod, initialPodUID, initialRestartCount),
			"baseline CSI driver pod %s/%s is not healthy before soak starts", csiNamespace, csiPod.Name,
		)
		// Capture OOM event watermark right after baseline health succeeds so OOMKills during
		// workload deploy/mount storm or warmup are never missed.
		eventCheckSince := time.Now()
		summary.InitialPodUID = string(initialPodUID)
		summary.InitialRestartCount = initialRestartCount
		framework.Logf("Baseline CSI pod %s/%s UID=%s restartCount=%d effectiveMemoryLimit=%s",
			csiNamespace, csiPod.Name, initialPodUID, initialRestartCount, effectiveLimit)

		ginkgo.By("Preparing test GCS bucket, Workload Identity, and 5x 1 MiB seed files")
		framework.ExpectNoError(ctx.Err(), "context cancelled before preparing test resources")
		// PrepareTest closes over the passed context in its own DeferCleanup for bucket and
		// service-account deletion, whereas Ginkgo cancels It's SpecContext upon exiting It
		// (even on passing runs). Detach cancellation so PrepareTest's cleanup still succeeds.
		config := driver.PrepareTest(context.WithoutCancel(ctx), f)
		config.Prefix = specs.EnableFileCacheAndMetricsPrefix
		volumeResource := storageframework.CreateVolumeResource(ctx, driver, config, pattern, e2evolume.SizeRange{})
		ginkgo.DeferCleanup(func(cleanupCtx ginkgo.SpecContext) {
			var cleanUpErrs []error
			cleanUpErrs = append(cleanUpErrs, volumeResource.CleanupResource(cleanupCtx))
			if cleanupErr := utilerrors.NewAggregate(cleanUpErrs); cleanupErr != nil {
				framework.Logf("Error cleaning up volume resource: %v", cleanupErr)
			}
		}, ginkgo.NodeTimeout(memorySoakCleanupTimeout))

		bucketName := volumeResource.VolSource.CSI.VolumeAttributes[csidriver.VolumeContextKeyBucketName]
		framework.Logf("Seeding %d x %d-byte files in bucket %q", memorySoakSeedFileCount, memorySoakSeedFileBytes, bucketName)
		seedSoakBucket(ctx, gcsfuseDriver, bucketName)

		deployCount, stsCount, jobCount := splitSoakPodCounts(MemorySoakPodCount)
		mountOpts := buildMemorySoakMountOptions(gcsfuseDriver.ClientProtocol)
		framework.Logf("Deploying %d soak workload pods on node %q (Deployment=%d, StatefulSet=%d, Job=%d) with mountOptions=%q",
			MemorySoakPodCount, targetNode, deployCount, stsCount, jobCount, mountOpts)

		// Register workload cleanup BEFORE a second collectDiag registration so Ginkgo's LIFO
		// DeferCleanup chain executes in order:
		// 1. late collectDiag (runs once via sync.Once while CSI pod & workload state are intact)
		// 2. cleanupSoakWorkloads (stops writes before bucket deletion)
		// 3. volumeResource.CleanupResource
		// 4. PrepareTest DeferCleanup (bucket/SA deletion)
		// 5. early collectDiag (no-op via sync.Once; runs only on earlier setup failure)
		// 6. f.dumpNamespaceInfo
		// 7. f.AfterEach
		ginkgo.DeferCleanup(func(cleanupCtx ginkgo.SpecContext) {
			cleanupSoakWorkloads(cleanupCtx, client, namespace, deployCount, stsCount, jobCount)
		}, ginkgo.NodeTimeout(memorySoakCleanupTimeout+30*time.Second))
		ginkgo.DeferCleanup(func(cleanupCtx ginkgo.SpecContext) {
			collectDiag(cleanupCtx)
		}, ginkgo.NodeTimeout(memorySoakDiagTimeout+30*time.Second))

		ginkgo.By(fmt.Sprintf("Deploying %d workload pods pinned to node %s", MemorySoakPodCount, targetNode))
		err = deploySoakWorkloads(ctx, client, namespace, targetNode, bucketName, mountOpts, deployCount, stsCount, jobCount)
		framework.ExpectNoError(err, "failed to deploy memory soak workloads")

		ginkgo.By(fmt.Sprintf("Waiting for all %d workload pods to become Running and Ready", MemorySoakPodCount))
		expectedPods, err := waitForSoakWorkloadsReady(ctx, client, namespace, targetNode, MemorySoakPodCount, memorySoakPodReadyTimeout)
		framework.ExpectNoError(err, "workload pods did not become Running and Ready in time")

		warmup := computeSoakWarmupDuration(MemorySoakDuration)
		ginkgo.By(fmt.Sprintf("Warming up workload caches and metrics pipelines for %s", warmup))
		if err := sleepWithContext(ctx, warmup); err != nil {
			framework.Failf("Context cancelled during warmup: %v", err)
		}

		soakStart := time.Now()
		soakEnd := soakStart.Add(MemorySoakDuration)
		ginkgo.By(fmt.Sprintf("Starting memory soak loop for %s (scrape interval %s, %d pods, limit %s)",
			MemorySoakDuration, MemorySoakScrapeInterval, MemorySoakPodCount, effectiveLimit))

		sampleIdx := 0
		for {
			iterStart := time.Now()
			sampleIdx++

			// 1. Actively scrape :9920/metrics via the Kubernetes API server pod proxy.
			// Before retrying any failed attempt, verify CSI driver container health so a real
			// OOMKill or pod restart aborts retries immediately instead of sleeping.
			csiHealthCheck := func(hcCtx context.Context) error {
				return checkCSIPodHealthBeforeScrapeRetry(
					hcCtx,
					client,
					csiNamespace,
					targetNode,
					initialPodUID,
					initialRestartCount,
					sampleIdx,
				)
			}
			scrapeBody, scrapeDur, scrapeAnalysis, scrapeErr := scrapeAndAnalyzeCSIMetrics(
				ctx,
				client,
				csiNamespace,
				csiPod.Name,
				namespace,
				expectedPods,
				sampleIdx == 1,
				csiHealthCheck,
			)

			// 2. Check CSI driver pod health immediately (even if scrape failed, so OOMKill is surfaced clearly).
			liveCSIPod, podErr := retryTransientK8sAPICall(ctx, "findActiveCSIPodOnNode", func(callCtx context.Context) (*corev1.Pod, error) {
				return findActiveCSIPodOnNode(callCtx, client, csiNamespace, targetNode)
			})
			if podErr != nil {
				framework.Failf("[sample %d] CSI driver pod on node %q missing or unreadable (scrapeErr=%v): %v",
					sampleIdx, targetNode, scrapeErr, podErr)
			}
			if healthErr := checkCSIContainerHealth(liveCSIPod, initialPodUID, initialRestartCount); healthErr != nil {
				framework.Failf("[sample %d] CSI driver container health check failed (scrapeErr=%v): %v",
					sampleIdx, scrapeErr, healthErr)
			}

			// 3. Check cluster events for any OOMKilling event referencing the CSI driver pod/process.
			oomEvents, evErr := listCSIOOMKillingEvents(ctx, client, liveCSIPod.Name, targetNode, eventCheckSince)
			if evErr != nil {
				framework.Logf("[sample %d] Warning: failed to list OOMKilling events: %v", sampleIdx, evErr)
			} else if len(oomEvents) > 0 {
				framework.Failf("[sample %d] Detected OOMKilling event(s) for CSI driver on node %q: %s",
					sampleIdx, targetNode, strings.Join(oomEvents, "; "))
			}

			// 4. Now gate on the :9920/metrics scrape result and pod label coverage.
			framework.ExpectNoError(scrapeErr, "[sample %d] failed to scrape %s/%s:9920/metrics", sampleIdx, csiNamespace, liveCSIPod.Name)
			if len(scrapeBody) == 0 {
				framework.Failf("[sample %d] received empty payload from %s/%s:9920/metrics", sampleIdx, csiNamespace, liveCSIPod.Name)
			}
			if scrapeAnalysis.DistinctPods != MemorySoakPodCount {
				framework.Failf("[sample %d] expected metrics from %d distinct soak pods in namespace %q, got %d (series=%d, bytes=%d, missing=%v)",
					sampleIdx, MemorySoakPodCount, namespace, scrapeAnalysis.DistinctPods,
					scrapeAnalysis.SeriesCount, scrapeAnalysis.Bytes, truncateStrings(scrapeAnalysis.MissingPods, 10))
			}

			// 5. Verify all workload pods remain Running and Ready on targetNode.
			workloadPods, listErr := retryTransientK8sAPICall(ctx, "list soak workload pods", func(callCtx context.Context) (*corev1.PodList, error) {
				return client.CoreV1().Pods(namespace).List(callCtx, metav1.ListOptions{
					LabelSelector: memorySoakWorkloadSelector,
				})
			})
			framework.ExpectNoError(listErr, "[sample %d] failed to list soak workload pods", sampleIdx)
			if wkErr := checkWorkloadPodsHealth(workloadPods.Items, MemorySoakPodCount, targetNode); wkErr != nil {
				framework.Failf("[sample %d] soak workload pods unhealthy: %v", sampleIdx, wkErr)
			}

			// 6. Best-effort non-gating telemetry from kubelet /stats/summary and /metrics/cadvisor.
			wsMiB, rssMiB, cgroupPeakMiB := collectNodeMemoryTelemetry(ctx, client, targetNode, csiNamespace, liveCSIPod.Name)
			cs, _ := getCSIContainerStatus(liveCSIPod)
			var currentRestarts int32
			if cs != nil {
				currentRestarts = cs.RestartCount
			}

			sample := memorySoakSample{
				SampleIndex:       sampleIdx,
				Timestamp:         iterStart.UTC().Format(time.RFC3339),
				ElapsedSeconds:    round2(time.Since(soakStart).Seconds()),
				ScrapeDurationMs:  scrapeDur.Milliseconds(),
				ScrapeBytes:       scrapeAnalysis.Bytes,
				ScrapeSeriesCount: scrapeAnalysis.SeriesCount,
				DistinctPods:      scrapeAnalysis.DistinctPods,
				WorkingSetMiB:     round2(wsMiB),
				RSSMiB:            round2(rssMiB),
				CgroupPeakMiB:     round2(cgroupPeakMiB),
				CSIRestartCount:   currentRestarts,
			}
			summary.Samples = append(summary.Samples, sample)
			summary.TotalSamples = len(summary.Samples)
			if sample.WorkingSetMiB > summary.MaxWorkingSetMiB {
				summary.MaxWorkingSetMiB = sample.WorkingSetMiB
			}
			if sample.RSSMiB > summary.MaxRSSMiB {
				summary.MaxRSSMiB = sample.RSSMiB
			}
			if sample.CgroupPeakMiB > summary.MaxCgroupPeakMiB {
				summary.MaxCgroupPeakMiB = sample.CgroupPeakMiB
			}

			framework.Logf("[memory-soak] sample=%d elapsed=%s/%s scrape=%dms bytes=%d series=%d pods=%d/%d ws=%.2fMiB rss=%.2fMiB cgroupPeak=%.2fMiB restarts=%d",
				sample.SampleIndex, time.Since(soakStart).Round(time.Second), MemorySoakDuration,
				sample.ScrapeDurationMs, sample.ScrapeBytes, sample.ScrapeSeriesCount,
				sample.DistinctPods, MemorySoakPodCount,
				sample.WorkingSetMiB, sample.RSSMiB, sample.CgroupPeakMiB, sample.CSIRestartCount)

			now := time.Now()
			if !now.Before(soakEnd) {
				break
			}

			nextTick := iterStart.Add(MemorySoakScrapeInterval)
			if nextTick.After(soakEnd) {
				nextTick = soakEnd
			}
			if sleepDur := time.Until(nextTick); sleepDur > 0 {
				if err := sleepWithContext(ctx, sleepDur); err != nil {
					framework.Failf("Context cancelled during memory soak loop: %v", err)
				}
			}
		}

		summary.Status = "passed"
		framework.Logf("Memory soak test PASSED after %d samples (maxWS=%.2fMiB, maxRSS=%.2fMiB, maxCgroupPeak=%.2fMiB, limit=%s, restarts=0)",
			summary.TotalSamples, summary.MaxWorkingSetMiB, summary.MaxRSSMiB, summary.MaxCgroupPeakMiB, effectiveLimit)
	})
}

// validateMemorySoakConfig checks soak runtime parameters and parses the memory limit quantity if set.
func validateMemorySoakConfig(duration, scrapeInterval time.Duration, podCount int, memLimit string) (resource.Quantity, error) {
	if podCount < 1 {
		return resource.Quantity{}, fmt.Errorf("soak pod count must be >= 1, got %d", podCount)
	}
	if duration <= 0 {
		return resource.Quantity{}, fmt.Errorf("soak duration must be > 0, got %s", duration)
	}
	if scrapeInterval <= 0 {
		return resource.Quantity{}, fmt.Errorf("soak scrape interval must be > 0, got %s", scrapeInterval)
	}
	if memLimit == "" {
		return resource.Quantity{}, nil
	}
	qty, err := resource.ParseQuantity(memLimit)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("invalid CSI driver memory limit %q: %w", memLimit, err)
	}
	if qty.Value() <= 0 {
		return resource.Quantity{}, fmt.Errorf("CSI driver memory limit must be > 0, got %q", memLimit)
	}
	return qty, nil
}

// splitSoakPodCounts divides total workload pods across Deployment, StatefulSet, and Job
// so that for 80 pods the distribution matches the 60 Deployment / 10 StatefulSet / 10 Job benchmark mix.
func splitSoakPodCounts(total int) (deployCount, stsCount, jobCount int) {
	if total <= 0 {
		return 0, 0, 0
	}
	if total < 3 {
		return total, 0, 0
	}
	stsCount = max(1, total/8)
	jobCount = max(1, total/8)
	deployCount = total - stsCount - jobCount
	return deployCount, stsCount, jobCount
}

// computeSoakWarmupDuration returns the pre-soak warmup duration, defaulting to 2m for normal soak runs
// and scaling down for short local smoke tests.
func computeSoakWarmupDuration(soakDuration time.Duration) time.Duration {
	if soakDuration >= memorySoakDefaultWarmup {
		return memorySoakDefaultWarmup
	}
	half := soakDuration / 2
	if half > 30*time.Second {
		return 30 * time.Second
	}
	return half
}

// buildMemorySoakMountOptions constructs the comma-separated mountOptions string for soak workload pods.
func buildMemorySoakMountOptions(clientProtocol string) string {
	opts := []string{
		"implicit-dirs",
		"file-cache:max-size-mb:20",
		"file-cache:cache-file-for-range-read:true",
		"metadata-cache:ttl-secs:60",
		"metadata-cache:stat-cache-max-size-mb:4",
		"metadata-cache:type-cache-max-size-mb:4",
	}
	if strings.EqualFold(clientProtocol, "grpc") {
		opts = append(opts, "client-protocol=grpc", "metrics:experimental-enable-grpc-metrics:true")
	}
	return strings.Join(opts, ",")
}

// selectSoakTargetNode deterministically selects one Ready, schedulable Linux node, preferring nodes
// without blocking taints and that are present in csiNodes (when non-empty).
func selectSoakTargetNode(nodes []corev1.Node, csiNodes map[string]bool) (string, error) {
	var preferred []string
	var fallback []string

	for _, node := range nodes {
		if node.Spec.Unschedulable {
			continue
		}
		if osLabel, ok := node.Labels["kubernetes.io/os"]; ok && osLabel != "linux" {
			continue
		}
		if !isNodeReady(&node) {
			continue
		}
		if len(csiNodes) > 0 && !csiNodes[node.Name] {
			continue
		}
		if hasBlockingTaint(node.Spec.Taints) {
			fallback = append(fallback, node.Name)
		} else {
			preferred = append(preferred, node.Name)
		}
	}

	sort.Strings(preferred)
	if len(preferred) > 0 {
		return preferred[0], nil
	}
	sort.Strings(fallback)
	if len(fallback) > 0 {
		return fallback[0], nil
	}
	return "", errors.New("no schedulable Ready Linux node running the CSI node driver found")
}

func isNodeReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func hasBlockingTaint(taints []corev1.Taint) bool {
	for _, taint := range taints {
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if taint.Key == "kubernetes.io/arch" && taint.Value == "arm64" {
			continue
		}
		return true
	}
	return false
}

func isPodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func getCSIContainerStatus(pod *corev1.Pod) (*corev1.ContainerStatus, error) {
	if pod == nil {
		return nil, errors.New("CSI driver pod is nil")
	}
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == memorySoakCSIContainerName {
			return &pod.Status.ContainerStatuses[i], nil
		}
	}
	return nil, fmt.Errorf("container %q not found in pod %s/%s", memorySoakCSIContainerName, pod.Namespace, pod.Name)
}

func extractCSIContainerSpecMemoryLimit(pod *corev1.Pod) (resource.Quantity, bool) {
	if pod == nil {
		return resource.Quantity{}, false
	}
	for _, c := range pod.Spec.Containers {
		if c.Name == memorySoakCSIContainerName {
			qty, ok := c.Resources.Limits[corev1.ResourceMemory]
			return qty, ok
		}
	}
	return resource.Quantity{}, false
}

func isOOMTerminated(term *corev1.ContainerStateTerminated) bool {
	if term == nil {
		return false
	}
	return term.Reason == "OOMKilled" || term.ExitCode == 137
}

// checkCSIContainerHealth asserts that the CSI node driver pod has not been recreated,
// has not restarted or been OOMKilled, and that gcs-fuse-csi-driver is Ready and Running.
func checkCSIContainerHealth(pod *corev1.Pod, expectedUID types.UID, expectedRestartCount int32) error {
	if pod == nil {
		return errors.New("CSI driver pod is nil")
	}
	if expectedUID != "" && pod.UID != expectedUID {
		return fmt.Errorf("CSI driver pod %s/%s was recreated: UID changed from %s to %s", pod.Namespace, pod.Name, expectedUID, pod.UID)
	}
	if pod.DeletionTimestamp != nil {
		return fmt.Errorf("CSI driver pod %s/%s is terminating", pod.Namespace, pod.Name)
	}
	if pod.Status.Phase != corev1.PodRunning {
		return fmt.Errorf("CSI driver pod %s/%s phase is %q, expected %q", pod.Namespace, pod.Name, pod.Status.Phase, corev1.PodRunning)
	}

	cs, err := getCSIContainerStatus(pod)
	if err != nil {
		return err
	}

	if isOOMTerminated(cs.State.Terminated) {
		return fmt.Errorf("container %q in %s/%s terminated with OOM (reason=%q, exitCode=%d, restartCount=%d)",
			cs.Name, pod.Namespace, pod.Name, cs.State.Terminated.Reason, cs.State.Terminated.ExitCode, cs.RestartCount)
	}
	if isOOMTerminated(cs.LastTerminationState.Terminated) {
		return fmt.Errorf("container %q in %s/%s was OOMKilled (lastReason=%q, lastExitCode=%d, restartCount=%d)",
			cs.Name, pod.Namespace, pod.Name, cs.LastTerminationState.Terminated.Reason, cs.LastTerminationState.Terminated.ExitCode, cs.RestartCount)
	}
	if cs.RestartCount != expectedRestartCount {
		lastInfo := ""
		if cs.LastTerminationState.Terminated != nil {
			lastInfo = fmt.Sprintf(" (lastReason=%q, lastExitCode=%d)",
				cs.LastTerminationState.Terminated.Reason, cs.LastTerminationState.Terminated.ExitCode)
		}
		return fmt.Errorf("container %q in %s/%s restarted: restartCount changed from %d to %d%s",
			cs.Name, pod.Namespace, pod.Name, expectedRestartCount, cs.RestartCount, lastInfo)
	}
	if !cs.Ready || cs.State.Running == nil {
		return fmt.Errorf("container %q in %s/%s is not ready/running (ready=%t, running=%t)",
			cs.Name, pod.Namespace, pod.Name, cs.Ready, cs.State.Running != nil)
	}
	return nil
}

// findCSIOOMKillingEvents filters Kubernetes events for OOMKilling events occurring at or after
// since that reference the CSI driver pod or process on targetNode.
func findCSIOOMKillingEvents(events []corev1.Event, csiPodName, targetNode string, since time.Time) []string {
	cutoff := since.Add(-5 * time.Second)
	var matches []string
	for _, ev := range events {
		if ev.Reason != "OOMKilling" {
			continue
		}
		evTime := eventTimestamp(&ev)
		if !evTime.IsZero() && evTime.Before(cutoff) {
			continue
		}
		if isCSIOOMEvent(&ev, csiPodName, targetNode) {
			matches = append(matches, fmt.Sprintf("[%s/%s %s] %s",
				ev.InvolvedObject.Kind, ev.InvolvedObject.Name, evTime.UTC().Format(time.RFC3339), ev.Message))
		}
	}
	return matches
}

func eventTimestamp(ev *corev1.Event) time.Time {
	if ev.Series != nil && !ev.Series.LastObservedTime.IsZero() {
		return ev.Series.LastObservedTime.Time
	}
	if !ev.LastTimestamp.IsZero() {
		return ev.LastTimestamp.Time
	}
	if !ev.EventTime.IsZero() {
		return ev.EventTime.Time
	}
	if !ev.FirstTimestamp.IsZero() {
		return ev.FirstTimestamp.Time
	}
	return ev.CreationTimestamp.Time
}

func isCSIOOMEvent(ev *corev1.Event, csiPodName, targetNode string) bool {
	if csiPodName != "" && ev.InvolvedObject.Name == csiPodName {
		return true
	}
	if ev.InvolvedObject.Kind == "Node" && (targetNode == "" || ev.InvolvedObject.Name == targetNode) {
		msg := ev.Message
		// Note: Linux TASK_COMM_LEN truncates "gcs-fuse-csi-driver" (19 chars) to "gcs-fuse-csi-dr" (15 chars) in kernel OOM logs.
		if (csiPodName != "" && strings.Contains(msg, csiPodName)) ||
			strings.Contains(msg, memorySoakCSIContainerName) ||
			strings.Contains(msg, "gcs-fuse-csi-dr") ||
			strings.Contains(msg, memorySoakCSIDaemonSetFallback) {
			return true
		}
	}
	return false
}

// checkWorkloadPodsHealth verifies that all expectedCount soak workload pods are Running, Ready,
// pinned to targetNode, and free of OOM terminations.
func checkWorkloadPodsHealth(pods []corev1.Pod, expectedCount int, targetNode string) error {
	if len(pods) != expectedCount {
		return fmt.Errorf("expected %d workload pods, found %d", expectedCount, len(pods))
	}
	var problems []string
	for i := range pods {
		p := &pods[i]
		if targetNode != "" && p.Spec.NodeName != targetNode {
			problems = append(problems, fmt.Sprintf("%s scheduled on %q (expected %q)", p.Name, p.Spec.NodeName, targetNode))
			continue
		}
		if p.Status.Phase != corev1.PodRunning || !isPodReady(p) {
			problems = append(problems, fmt.Sprintf("%s phase=%s ready=%t", p.Name, p.Status.Phase, isPodReady(p)))
			continue
		}
		for _, cs := range append(append([]corev1.ContainerStatus{}, p.Status.InitContainerStatuses...), p.Status.ContainerStatuses...) {
			if isOOMTerminated(cs.State.Terminated) || isOOMTerminated(cs.LastTerminationState.Terminated) {
				problems = append(problems, fmt.Sprintf("%s container %s OOMKilled", p.Name, cs.Name))
			}
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%d/%d workload pods unhealthy: %s", len(problems), expectedCount, strings.Join(truncateStrings(problems, 5), "; "))
	}
	return nil
}

// analyzeScrapePayload scans Prometheus exposition text from :9920/metrics, counting non-comment
// metric series and distinct pod_name labels belonging to namespace (and expectedPods when non-empty).
func analyzeScrapePayload(payload []byte, namespace string, expectedPods map[string]struct{}) memorySoakScrapeAnalysis {
	res := memorySoakScrapeAnalysis{
		Bytes: len(payload),
	}
	if len(payload) == 0 {
		return res
	}

	seenPods := make(map[string]struct{})
	scanner := bufio.NewScanner(bytes.NewReader(payload))
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		_, labels, _, ok := parsePromSampleLine(scanner.Text())
		if !ok {
			continue
		}
		res.SeriesCount++
		if labels == "" {
			continue
		}
		if namespace != "" {
			nsVal, hasNS := extractPromLabelValue(labels, "namespace_name")
			if !hasNS || nsVal != namespace {
				continue
			}
		}
		podVal, hasPod := extractPromLabelValue(labels, "pod_name")
		if !hasPod || podVal == "" {
			continue
		}
		if len(expectedPods) > 0 {
			if _, wanted := expectedPods[podVal]; !wanted {
				continue
			}
		}
		seenPods[podVal] = struct{}{}
	}

	res.DistinctPods = len(seenPods)
	if len(expectedPods) > 0 && len(seenPods) < len(expectedPods) {
		missing := make([]string, 0, len(expectedPods)-len(seenPods))
		for podName := range expectedPods {
			if _, ok := seenPods[podName]; !ok {
				missing = append(missing, podName)
			}
		}
		sort.Strings(missing)
		res.MissingPods = missing
	}
	return res
}

// parseStatsSummaryMemory extracts workingSetBytes and rssBytes for the specified CSI pod container
// from kubelet's /stats/summary JSON response.
func parseStatsSummaryMemory(data []byte, csiNamespace, csiPodName, containerName string) (workingSetBytes, rssBytes uint64, found bool, err error) {
	var summary kubeletStatsSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		return 0, 0, false, fmt.Errorf("failed to parse kubelet stats/summary JSON: %w", err)
	}
	for _, pod := range summary.Pods {
		if pod.PodRef.Name != csiPodName {
			continue
		}
		if csiNamespace != "" && pod.PodRef.Namespace != csiNamespace {
			continue
		}
		for _, c := range pod.Containers {
			if c.Name != containerName {
				continue
			}
			if c.Memory != nil {
				if c.Memory.WorkingSetBytes != nil {
					workingSetBytes = *c.Memory.WorkingSetBytes
				}
				if c.Memory.RSSBytes != nil {
					rssBytes = *c.Memory.RSSBytes
				}
			}
			return workingSetBytes, rssBytes, true, nil
		}
	}
	return 0, 0, false, nil
}

// parseCadvisorMemoryPeak extracts the maximum container_memory_max_usage_bytes (cgroup memory.peak)
// value for the specified CSI pod container from kubelet's /metrics/cadvisor Prometheus text output.
func parseCadvisorMemoryPeak(data []byte, csiNamespace, csiPodName, containerName string) (maxUsageBytes uint64, found bool) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "container_memory_max_usage_bytes{") {
			continue
		}
		metricName, labels, rest, ok := parsePromSampleLine(line)
		if !ok || metricName != "container_memory_max_usage_bytes" || labels == "" {
			continue
		}
		cVal, ok := extractPromLabelValue(labels, "container")
		if !ok || cVal != containerName {
			continue
		}
		pVal, ok := extractPromLabelValue(labels, "pod")
		if !ok || pVal != csiPodName {
			continue
		}
		if csiNamespace != "" {
			nsVal, ok := extractPromLabelValue(labels, "namespace")
			if !ok || nsVal != csiNamespace {
				continue
			}
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		valFloat, err := strconv.ParseFloat(fields[0], 64)
		if err != nil || math.IsNaN(valFloat) || math.IsInf(valFloat, 0) || valFloat < 0 {
			continue
		}
		valBytes := uint64(valFloat)
		if !found || valBytes > maxUsageBytes {
			maxUsageBytes = valBytes
			found = true
		}
	}
	return maxUsageBytes, found
}

// parsePromSampleLine parses a single non-comment Prometheus exposition sample line into
// metricName, labels substring (without enclosing braces), and trailing value/timestamp text.
func parsePromSampleLine(line string) (metricName, labels, rest string, ok bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return "", "", "", false
	}

	openBrace := strings.IndexByte(trimmed, '{')
	if openBrace < 0 {
		idx := strings.IndexAny(trimmed, " \t")
		if idx < 0 {
			return "", "", "", false
		}
		return trimmed[:idx], "", strings.TrimSpace(trimmed[idx+1:]), true
	}

	inQuotes := false
	escaped := false
	closeBrace := -1
	for i := openBrace + 1; i < len(trimmed); i++ {
		ch := trimmed[i]
		if inQuotes {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
			} else if ch == '"' {
				inQuotes = false
			}
			continue
		}
		if ch == '"' {
			inQuotes = true
		} else if ch == '}' {
			closeBrace = i
			break
		}
	}
	if closeBrace < 0 {
		return "", "", "", false
	}
	return strings.TrimSpace(trimmed[:openBrace]), trimmed[openBrace+1 : closeBrace], strings.TrimSpace(trimmed[closeBrace+1:]), true
}

// extractPromLabelValue extracts the unescaped value of targetKey from a Prometheus labels block
// (the contents inside `{...}`).
func extractPromLabelValue(labels, targetKey string) (string, bool) {
	i := 0
	n := len(labels)
	for i < n {
		for i < n && (labels[i] == ' ' || labels[i] == '\t' || labels[i] == ',') {
			i++
		}
		if i >= n {
			break
		}
		keyStart := i
		for i < n && labels[i] != '=' && labels[i] != ',' {
			i++
		}
		if i >= n || labels[i] != '=' {
			break
		}
		key := strings.TrimSpace(labels[keyStart:i])
		i++ // skip '='
		for i < n && (labels[i] == ' ' || labels[i] == '\t') {
			i++
		}
		if i >= n || labels[i] != '"' {
			break
		}
		i++ // skip opening '"'
		valStart := i
		hasEscape := false
		for i < n {
			if labels[i] == '\\' {
				hasEscape = true
				i += 2
				continue
			}
			if labels[i] == '"' {
				break
			}
			i++
		}
		if i >= n {
			break
		}
		rawVal := labels[valStart:i]
		i++ // skip closing '"'
		if key == targetKey {
			if !hasEscape {
				return rawVal, true
			}
			return unescapePromLabelValue(rawVal), true
		}
	}
	return "", false
}

func unescapePromLabelValue(raw string) string {
	var b strings.Builder
	b.Grow(len(raw))
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\\' && i+1 < len(raw) {
			switch raw[i+1] {
			case 'n':
				b.WriteByte('\n')
			case '"':
				b.WriteByte('"')
			case '\\':
				b.WriteByte('\\')
			default:
				b.WriteByte('\\')
				b.WriteByte(raw[i+1])
			}
			i++
			continue
		}
		b.WriteByte(raw[i])
	}
	return b.String()
}

func isActiveCSIPod(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Spec.NodeName == "" {
		return false
	}
	return pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded
}

// isBetterCSIPodCandidate deterministically ranks active CSI driver pods on the same node,
// preferring PodRunning, then newest CreationTimestamp, then lexicographically smaller Name.
func isBetterCSIPodCandidate(candidate, current *corev1.Pod) bool {
	if current == nil {
		return true
	}
	candRunning := candidate.Status.Phase == corev1.PodRunning
	currRunning := current.Status.Phase == corev1.PodRunning
	if candRunning != currRunning {
		return candRunning
	}
	if !candidate.CreationTimestamp.Equal(&current.CreationTimestamp) {
		return candidate.CreationTimestamp.After(current.CreationTimestamp.Time)
	}
	return candidate.Name < current.Name
}

func selectTargetNodeAndCSIPod(ctx context.Context, client clientset.Interface) (string, *corev1.Pod, error) {
	csiPods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: memorySoakCSIAppLabel,
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to list CSI node driver pods: %w", err)
	}
	csiPodByNode := make(map[string]*corev1.Pod, len(csiPods.Items))
	csiNodes := make(map[string]bool, len(csiPods.Items))
	for i := range csiPods.Items {
		pod := &csiPods.Items[i]
		if !isActiveCSIPod(pod) {
			continue
		}
		if existing, ok := csiPodByNode[pod.Spec.NodeName]; !ok || isBetterCSIPodCandidate(pod, existing) {
			csiPodByNode[pod.Spec.NodeName] = pod
		}
		csiNodes[pod.Spec.NodeName] = true
	}

	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "kubernetes.io/os=linux",
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to list Linux nodes: %w", err)
	}

	targetNode, err := selectSoakTargetNode(nodes.Items, csiNodes)
	if err != nil {
		return "", nil, err
	}
	csiPod, ok := csiPodByNode[targetNode]
	if !ok {
		return "", nil, fmt.Errorf("no active CSI node driver pod found on selected node %q", targetNode)
	}
	return targetNode, csiPod, nil
}

func resolveCSIDaemonSetName(pod *corev1.Pod) string {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "DaemonSet" && owner.Name != "" {
			return owner.Name
		}
	}
	return memorySoakCSIDaemonSetFallback
}

func findActiveCSIPodOnNode(ctx context.Context, client clientset.Interface, namespace, nodeName string) (*corev1.Pod, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: memorySoakCSIAppLabel,
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return nil, err
	}
	var active []*corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !isActiveCSIPod(pod) {
			continue
		}
		if nodeName != "" && pod.Spec.NodeName != nodeName {
			continue
		}
		active = append(active, pod)
	}
	if len(active) == 0 {
		return nil, fmt.Errorf("no active CSI driver pod found in namespace %q on node %q", namespace, nodeName)
	}
	if len(active) > 1 {
		sort.Slice(active, func(i, j int) bool {
			return isBetterCSIPodCandidate(active[i], active[j])
		})
	}
	return active[0], nil
}

func ensureCSIDriverMemoryLimit(
	ctx context.Context,
	client clientset.Interface,
	csiNamespace, dsName, targetNode string,
	currentPod *corev1.Pod,
	requestedLimitStr string,
	desiredLimit resource.Quantity,
) (*corev1.Pod, string, error) {
	actualLimit, hasActualLimit := extractCSIContainerSpecMemoryLimit(currentPod)
	actualLimitStr := "unlimited"
	if hasActualLimit {
		actualLimitStr = actualLimit.String()
	}

	if requestedLimitStr == "" {
		framework.Logf("No CSI driver memory limit override configured; using live limit %s on %s/%s", actualLimitStr, csiNamespace, currentPod.Name)
		return currentPod, actualLimitStr, nil
	}

	if csiNamespace == memorySoakManagedNamespace {
		if !hasActualLimit || actualLimit.Cmp(desiredLimit) != 0 {
			framework.Logf("WARNING: Detected GKE managed CSI driver in %q with memory limit %s (requested %s); skipping DaemonSet patch on AddonManager-reconciled DaemonSet",
				csiNamespace, actualLimitStr, desiredLimit.String())
		} else {
			framework.Logf("Detected GKE managed CSI driver in %q matching requested memory limit %s", csiNamespace, actualLimitStr)
		}
		return currentPod, actualLimitStr, nil
	}

	previousPodUID := currentPod.UID
	var dsModified bool
	var targetGeneration int64

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		ds, getErr := client.AppsV1().DaemonSets(csiNamespace).Get(ctx, dsName, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		dsModified = false
		foundContainer := false
		for i := range ds.Spec.Template.Spec.Containers {
			c := &ds.Spec.Template.Spec.Containers[i]
			if c.Name != memorySoakCSIContainerName {
				continue
			}
			foundContainer = true
			if c.Resources.Limits == nil {
				c.Resources.Limits = corev1.ResourceList{}
			}
			curLim, hasLim := c.Resources.Limits[corev1.ResourceMemory]
			if !hasLim || curLim.Cmp(desiredLimit) != 0 {
				c.Resources.Limits[corev1.ResourceMemory] = desiredLimit.DeepCopy()
				dsModified = true
			}
			if curReq, hasReq := c.Resources.Requests[corev1.ResourceMemory]; hasReq && curReq.Cmp(desiredLimit) > 0 {
				c.Resources.Requests[corev1.ResourceMemory] = desiredLimit.DeepCopy()
				dsModified = true
			}
			break
		}
		if !foundContainer {
			return fmt.Errorf("container %q not found in DaemonSet %s/%s", memorySoakCSIContainerName, csiNamespace, dsName)
		}
		if !dsModified {
			targetGeneration = ds.Generation
			return nil
		}
		updated, updateErr := client.AppsV1().DaemonSets(csiNamespace).Update(ctx, ds, metav1.UpdateOptions{})
		if updateErr != nil {
			return updateErr
		}
		targetGeneration = updated.Generation
		return nil
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to update DaemonSet %s/%s memory limit to %s: %w", csiNamespace, dsName, desiredLimit.String(), err)
	}

	if dsModified {
		framework.Logf("Patched DaemonSet %s/%s container %q memory limit to %s (generation %d); waiting for rollout...",
			csiNamespace, dsName, memorySoakCSIContainerName, desiredLimit.String(), targetGeneration)
	} else {
		framework.Logf("DaemonSet %s/%s container %q already has memory limit %s",
			csiNamespace, dsName, memorySoakCSIContainerName, desiredLimit.String())
	}

	var rolledPod *corev1.Pod
	pollErr := wait.PollUntilContextTimeout(ctx, memorySoakRolloutPollInterval, memorySoakRolloutTimeout, true, func(ctx context.Context) (bool, error) {
		ds, err := client.AppsV1().DaemonSets(csiNamespace).Get(ctx, dsName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		if ds.Status.ObservedGeneration < targetGeneration ||
			ds.Status.DesiredNumberScheduled == 0 ||
			ds.Status.UpdatedNumberScheduled != ds.Status.DesiredNumberScheduled ||
			ds.Status.NumberAvailable != ds.Status.DesiredNumberScheduled ||
			ds.Status.NumberUnavailable > 0 {
			return false, nil
		}
		pod, err := findActiveCSIPodOnNode(ctx, client, csiNamespace, targetNode)
		if err != nil {
			return false, nil
		}
		if dsModified && pod.UID == previousPodUID {
			return false, nil
		}
		if pod.Status.Phase != corev1.PodRunning || !isPodReady(pod) {
			return false, nil
		}
		lim, ok := extractCSIContainerSpecMemoryLimit(pod)
		if !ok || lim.Cmp(desiredLimit) != 0 {
			return false, nil
		}
		rolledPod = pod
		return true, nil
	})
	if pollErr != nil {
		return nil, "", fmt.Errorf("timed out waiting for DaemonSet %s/%s rollout with memory limit %s on node %q: %w",
			csiNamespace, dsName, desiredLimit.String(), targetNode, pollErr)
	}

	if dsModified || rolledPod.UID != previousPodUID {
		// Allow kubelet's CSI plugin watcher a brief settle window after DaemonSet restart,
		// then re-fetch the live pod to verify it did not OOMKill or recreate during settle.
		if err := sleepWithContext(ctx, memorySoakRolloutSettleDuration); err != nil {
			return nil, "", err
		}
		refreshedPod, err := retryTransientK8sAPICall(ctx, "post-rollout findActiveCSIPodOnNode", func(callCtx context.Context) (*corev1.Pod, error) {
			return findActiveCSIPodOnNode(callCtx, client, csiNamespace, targetNode)
		})
		if err != nil {
			return nil, "", fmt.Errorf("failed to re-fetch CSI pod on node %q after rollout settle: %w", targetNode, err)
		}
		if refreshedPod.UID != rolledPod.UID {
			return nil, "", fmt.Errorf("CSI pod on node %q changed UID during rollout settle (from %s to %s)",
				targetNode, rolledPod.UID, refreshedPod.UID)
		}
		if err := checkCSIContainerHealth(refreshedPod, rolledPod.UID, 0); err != nil {
			return nil, "", fmt.Errorf("CSI pod %s/%s unhealthy after rollout settle: %w",
				csiNamespace, refreshedPod.Name, err)
		}
		rolledPod = refreshedPod
	}

	finalLimit, ok := extractCSIContainerSpecMemoryLimit(rolledPod)
	if !ok || finalLimit.Cmp(desiredLimit) != 0 {
		return nil, "", fmt.Errorf("live CSI pod %s/%s memory limit %s does not match requested %s",
			csiNamespace, rolledPod.Name, finalLimit.String(), desiredLimit.String())
	}
	return rolledPod, finalLimit.String(), nil
}

func seedSoakBucket(ctx context.Context, gcsfuseDriver *specs.GCSFuseCSITestDriver, bucketName string) {
	buf := make([]byte, memorySoakSeedFileBytes)
	for seedIdx := 1; seedIdx <= memorySoakSeedFileCount; seedIdx++ {
		for j := range buf {
			buf[j] = byte((j*31 + seedIdx*17) & 0xff)
		}
		objectName := fmt.Sprintf("perf-shared/seed_%d.bin", seedIdx)
		gcsfuseDriver.CreateTestObjectInBucket(ctx, objectName, bucketName, buf)
	}
}

func buildSoakPodTemplateSpec(targetNode, bucketName, mountOpts string, labels map[string]string, restartPolicy corev1.RestartPolicy) corev1.PodTemplateSpec {
	podLabels := make(map[string]string, len(labels)+1)
	for k, v := range labels {
		podLabels[k] = v
	}
	podLabels[memorySoakWorkloadLabelKey] = memorySoakWorkloadLabelValue

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: podLabels,
			Annotations: map[string]string{
				"gke-gcsfuse/volumes":                   "true",
				"gke-gcsfuse/ephemeral-storage-request": "200Mi",
				"gke-gcsfuse/ephemeral-storage-limit":   "200Mi",
			},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:            specs.K8sServiceAccountName,
			AutomountServiceAccountToken:  ptr.To(false),
			RestartPolicy:                 restartPolicy,
			TerminationGracePeriodSeconds: ptr.To(int64(5)),
			NodeSelector: map[string]string{
				"kubernetes.io/os":       "linux",
				"kubernetes.io/hostname": targetNode,
			},
			Tolerations: []corev1.Toleration{
				{
					Key:      "kubernetes.io/arch",
					Operator: corev1.TolerationOpEqual,
					Value:    "arm64",
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
			Containers: []corev1.Container{
				{
					Name:    "io-worker",
					Image:   imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{"/bin/sh", "-c"},
					Args:    []string{memorySoakIOScript},
					Env: []corev1.EnvVar{
						{
							Name: "POD_NAME",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
							},
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10m"),
							corev1.ResourceMemory: resource.MustParse("32Mi"),
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      memorySoakVolumeName,
							MountPath: memorySoakMountPath,
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: memorySoakVolumeName,
					VolumeSource: corev1.VolumeSource{
						CSI: &corev1.CSIVolumeSource{
							Driver: csidriver.DefaultName,
							VolumeAttributes: map[string]string{
								csidriver.VolumeContextKeyBucketName:                bucketName,
								csidriver.VolumeContextKeyMountOptions:              mountOpts,
								csidriver.VolumeContextKeyDisableMetrics:            "false",
								csidriver.VolumeContextKeyFileCacheCapacity:         "20Mi",
								csidriver.VolumeContextKeyFileCacheForRangeRead:     "true",
								csidriver.VolumeContextKeyMetadataStatCacheCapacity: "4Mi",
								csidriver.VolumeContextKeyMetadataTypeCacheCapacity: "4Mi",
								csidriver.VolumeContextKeyMetadataCacheTTLSeconds:   "60",
							},
						},
					},
				},
			},
		},
	}
}

func deploySoakWorkloads(ctx context.Context, client clientset.Interface, namespace, targetNode, bucketName, mountOpts string, deployCount, stsCount, jobCount int) error {
	if deployCount > 0 {
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      memorySoakDeployName,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(int32(deployCount)),
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": memorySoakDeployName},
				},
				Template: buildSoakPodTemplateSpec(targetNode, bucketName, mountOpts, map[string]string{
					"app": memorySoakDeployName,
				}, corev1.RestartPolicyAlways),
			},
		}
		if _, err := client.AppsV1().Deployments(namespace).Create(ctx, deploy, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create Deployment %s: %w", memorySoakDeployName, err)
		}
	}

	if stsCount > 0 {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      memorySoakStsSvcName,
				Namespace: namespace,
			},
			Spec: corev1.ServiceSpec{
				ClusterIP: corev1.ClusterIPNone,
				Selector:  map[string]string{"app": memorySoakStsName},
			},
		}
		if _, err := client.CoreV1().Services(namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create headless Service %s: %w", memorySoakStsSvcName, err)
		}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      memorySoakStsName,
				Namespace: namespace,
			},
			Spec: appsv1.StatefulSetSpec{
				ServiceName:         memorySoakStsSvcName,
				Replicas:            ptr.To(int32(stsCount)),
				PodManagementPolicy: appsv1.ParallelPodManagement,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": memorySoakStsName},
				},
				Template: buildSoakPodTemplateSpec(targetNode, bucketName, mountOpts, map[string]string{
					"app": memorySoakStsName,
				}, corev1.RestartPolicyAlways),
			},
		}
		if _, err := client.AppsV1().StatefulSets(namespace).Create(ctx, sts, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create StatefulSet %s: %w", memorySoakStsName, err)
		}
	}

	if jobCount > 0 {
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      memorySoakJobName,
				Namespace: namespace,
			},
			Spec: batchv1.JobSpec{
				Parallelism: ptr.To(int32(jobCount)),
				Completions: ptr.To(int32(jobCount)),
				Template: buildSoakPodTemplateSpec(targetNode, bucketName, mountOpts, map[string]string{
					"app":                 memorySoakJobName,
					memorySoakJobSetLabel: memorySoakJobName,
				}, corev1.RestartPolicyOnFailure),
			},
		}
		if _, err := client.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create Job %s: %w", memorySoakJobName, err)
		}
	}

	return nil
}

func waitForSoakWorkloadsReady(ctx context.Context, client clientset.Interface, namespace, targetNode string, expectedCount int, timeout time.Duration) (map[string]struct{}, error) {
	var lastReady int
	var expectedPods map[string]struct{}
	pollErr := wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: memorySoakWorkloadSelector,
		})
		if err != nil {
			framework.Logf("Transient error listing soak workload pods: %v", err)
			return false, nil
		}
		readyCount := 0
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.Status.Phase == corev1.PodRunning && isPodReady(p) && p.Spec.NodeName == targetNode {
				readyCount++
			}
		}
		if readyCount != lastReady {
			framework.Logf("Soak workload pods Ready on node %q: %d/%d (total listed: %d)", targetNode, readyCount, expectedCount, len(pods.Items))
			lastReady = readyCount
		}
		if len(pods.Items) == expectedCount && readyCount == expectedCount {
			expectedPods = make(map[string]struct{}, len(pods.Items))
			for _, p := range pods.Items {
				expectedPods[p.Name] = struct{}{}
			}
			return true, nil
		}
		return false, nil
	})
	if pollErr != nil {
		logUnreadySoakPods(ctx, client, namespace)
		return nil, fmt.Errorf("expected %d Running & Ready soak workload pods on node %q within %s (last ready=%d): %w",
			expectedCount, targetNode, timeout, lastReady, pollErr)
	}
	return expectedPods, nil
}

func logUnreadySoakPods(ctx context.Context, client clientset.Interface, namespace string) {
	diagCtx, cancel := context.WithTimeout(ctx, memorySoakDiagTimeout)
	defer cancel()
	pods, err := client.CoreV1().Pods(namespace).List(diagCtx, metav1.ListOptions{
		LabelSelector: memorySoakWorkloadSelector,
	})
	if err != nil {
		framework.Logf("Failed to list soak pods for unready diagnostics: %v", err)
		return
	}
	logged := 0
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodRunning && isPodReady(p) {
			continue
		}
		if logged >= 10 {
			break
		}
		logged++
		var condMsgs []string
		for _, c := range p.Status.Conditions {
			if c.Status != corev1.ConditionTrue && c.Message != "" {
				condMsgs = append(condMsgs, fmt.Sprintf("%s=%s(%s:%s)", c.Type, c.Status, c.Reason, c.Message))
			}
		}
		framework.Logf("Unready soak pod %s: phase=%s node=%q conditions=[%s]", p.Name, p.Status.Phase, p.Spec.NodeName, strings.Join(condMsgs, ", "))
	}
}

func cleanupSoakWorkloads(ctx context.Context, client clientset.Interface, namespace string, deployCount, stsCount, jobCount int) {
	cleanupCtx, cancel := context.WithTimeout(ctx, memorySoakCleanupTimeout)
	defer cancel()

	bg := metav1.DeletePropagationBackground
	delOpts := metav1.DeleteOptions{PropagationPolicy: &bg}

	if deployCount > 0 {
		if err := client.AppsV1().Deployments(namespace).Delete(cleanupCtx, memorySoakDeployName, delOpts); err != nil && !apierrors.IsNotFound(err) {
			framework.Logf("Error deleting Deployment %s/%s: %v", namespace, memorySoakDeployName, err)
		}
	}
	if stsCount > 0 {
		if err := client.AppsV1().StatefulSets(namespace).Delete(cleanupCtx, memorySoakStsName, delOpts); err != nil && !apierrors.IsNotFound(err) {
			framework.Logf("Error deleting StatefulSet %s/%s: %v", namespace, memorySoakStsName, err)
		}
		if err := client.CoreV1().Services(namespace).Delete(cleanupCtx, memorySoakStsSvcName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			framework.Logf("Error deleting Service %s/%s: %v", namespace, memorySoakStsSvcName, err)
		}
	}
	if jobCount > 0 {
		if err := client.BatchV1().Jobs(namespace).Delete(cleanupCtx, memorySoakJobName, delOpts); err != nil && !apierrors.IsNotFound(err) {
			framework.Logf("Error deleting Job %s/%s: %v", namespace, memorySoakJobName, err)
		}
	}

	// Wait for workload pods to terminate so mounts stop writing before GCS bucket deletion.
	err := wait.PollUntilContextTimeout(cleanupCtx, 5*time.Second, memorySoakCleanupTimeout-10*time.Second, true, func(ctx context.Context) (bool, error) {
		pods, listErr := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: memorySoakWorkloadSelector,
		})
		if listErr != nil {
			return false, nil
		}
		return len(pods.Items) == 0, nil
	})
	if err != nil {
		framework.Logf("Warning: timed out waiting for soak workload pods in %q to terminate: %v", namespace, err)
	}
}

type csiMetricsScrapeFunc func(ctx context.Context) ([]byte, error)
type csiHealthCheckFunc func(ctx context.Context) error

func isTransientK8sAPIError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) ||
		apierrors.IsServiceUnavailable(err) ||
		apierrors.IsInternalError(err) ||
		apierrors.IsUnexpectedServerError(err) {
		return true
	}
	var statusErr apierrors.APIStatus
	if errors.As(err, &statusErr) {
		if code := statusErr.Status().Code; code >= http.StatusInternalServerError && code != http.StatusNotImplemented {
			return true
		}
	}
	return utilnet.IsConnectionRefused(err) ||
		utilnet.IsConnectionReset(err) ||
		utilnet.IsHTTP2ConnectionLost(err) ||
		utilnet.IsProbableEOF(err) ||
		utilnet.IsTimeout(err)
}

func checkCSIPodHealthBeforeScrapeRetry(
	hcCtx context.Context,
	client clientset.Interface,
	csiNamespace, targetNode string,
	initialPodUID types.UID,
	initialRestartCount int32,
	sampleIdx int,
) error {
	if hcCtx.Err() != nil {
		return hcCtx.Err()
	}
	callCtx, cancel := context.WithTimeout(hcCtx, memorySoakDiagTimeout)
	defer cancel()
	pod, podErr := findActiveCSIPodOnNode(callCtx, client, csiNamespace, targetNode)
	if podErr != nil {
		if hcCtx.Err() == nil && isTransientK8sAPIError(podErr) {
			framework.Logf("[sample %d] Transient API error checking CSI pod health before scrape retry: %v", sampleIdx, podErr)
			return nil
		}
		if hcCtx.Err() != nil {
			return hcCtx.Err()
		}
		return podErr
	}
	return checkCSIContainerHealth(pod, initialPodUID, initialRestartCount)
}

// retryTransientK8sAPICall runs fn with a per-call memorySoakDiagTimeout up to memorySoakAPIMaxAttempts
// times, retrying only transient Kubernetes API transport/server errors (and never retrying when the
// parent context is cancelled or when fn returns a non-transient error).
func retryTransientK8sAPICall[T any](ctx context.Context, opName string, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	var lastErr error
	for attempt := 1; attempt <= memorySoakAPIMaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		callCtx, cancel := context.WithTimeout(ctx, memorySoakDiagTimeout)
		res, err := fn(callCtx)
		cancel()
		if err == nil {
			return res, nil
		}
		lastErr = err
		if ctx.Err() != nil || !isTransientK8sAPIError(err) || attempt == memorySoakAPIMaxAttempts {
			return zero, err
		}
		framework.Logf("Transient Kubernetes API error during %s (attempt %d/%d): %v; retrying in %s",
			opName, attempt, memorySoakAPIMaxAttempts, err, memorySoakAPIRetryDelay)
		if sleepErr := sleepWithContext(ctx, memorySoakAPIRetryDelay); sleepErr != nil {
			return zero, sleepErr
		}
	}
	return zero, lastErr
}

func scrapeAndAnalyzeCSIMetrics(
	ctx context.Context,
	client clientset.Interface,
	csiNamespace, csiPodName, workloadNamespace string,
	expectedPods map[string]struct{},
	isFirstScrape bool,
	healthCheck csiHealthCheckFunc,
) ([]byte, time.Duration, memorySoakScrapeAnalysis, error) {
	scrapeFn := func(scrapeCtx context.Context) ([]byte, error) {
		return client.CoreV1().RESTClient().Get().
			Namespace(csiNamespace).
			Resource("pods").
			Name(csiPodName + ":9920").
			SubResource("proxy").
			Suffix("metrics").
			DoRaw(scrapeCtx)
	}
	return scrapeAndAnalyzeCSIMetricsWithFunc(ctx, scrapeFn, workloadNamespace, expectedPods, isFirstScrape, healthCheck)
}

func scrapeAndAnalyzeCSIMetricsWithFunc(
	ctx context.Context,
	scrapeFn csiMetricsScrapeFunc,
	workloadNamespace string,
	expectedPods map[string]struct{},
	isFirstScrape bool,
	healthCheck csiHealthCheckFunc,
) ([]byte, time.Duration, memorySoakScrapeAnalysis, error) {
	maxAttempts := memorySoakSteadyScrapeMaxAttempts
	retryDelay := memorySoakSteadyScrapeRetryDelay
	if isFirstScrape {
		maxAttempts = memorySoakFirstScrapeMaxAttempts
		retryDelay = memorySoakFirstScrapeRetryDelay
	}

	var lastBody []byte
	var lastDur time.Duration
	var lastAnalysis memorySoakScrapeAnalysis
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return lastBody, lastDur, lastAnalysis, ctx.Err()
		}
		lastAnalysis = memorySoakScrapeAnalysis{}
		scrapeCtx, cancel := context.WithTimeout(ctx, memorySoakScrapeTimeout)
		start := time.Now()
		body, err := scrapeFn(scrapeCtx)
		dur := time.Since(start)
		cancel()

		lastBody = body
		lastDur = dur
		lastErr = err
		if err == nil {
			lastAnalysis = analyzeScrapePayload(body, workloadNamespace, expectedPods)
			if len(body) > 0 && lastAnalysis.DistinctPods == len(expectedPods) {
				return lastBody, lastDur, lastAnalysis, nil
			}
		}

		if ctx.Err() != nil {
			return lastBody, lastDur, lastAnalysis, ctx.Err()
		}

		if attempt < maxAttempts {
			if healthCheck != nil {
				if healthErr := healthCheck(ctx); healthErr != nil {
					if ctx.Err() != nil {
						return lastBody, lastDur, lastAnalysis, ctx.Err()
					}
					framework.Logf("Aborting :9920/metrics scrape retries on attempt %d/%d because CSI driver is unhealthy: %v",
						attempt, maxAttempts, healthErr)
					if lastErr != nil {
						return lastBody, lastDur, lastAnalysis, fmt.Errorf("scrape failed (%w) and CSI driver unhealthy: %w", lastErr, healthErr)
					}
					return lastBody, lastDur, lastAnalysis, fmt.Errorf("CSI driver unhealthy during metrics scrape: %w", healthErr)
				}
			}
			prefix := ""
			if isFirstScrape {
				prefix = "Initial "
			}
			framework.Logf("%s:9920/metrics scrape attempt %d/%d incomplete (err=%v, bytes=%d, distinctPods=%d/%d); retrying in %s",
				prefix, attempt, maxAttempts, err, len(body), lastAnalysis.DistinctPods, len(expectedPods), retryDelay)
			if sleepErr := sleepWithContext(ctx, retryDelay); sleepErr != nil {
				return lastBody, lastDur, lastAnalysis, sleepErr
			}
		}
	}

	return lastBody, lastDur, lastAnalysis, lastErr
}

func listCSIOOMKillingEvents(ctx context.Context, client clientset.Interface, csiPodName, targetNode string, since time.Time) ([]string, error) {
	events, err := retryTransientK8sAPICall(ctx, "list OOMKilling events", func(callCtx context.Context) (*corev1.EventList, error) {
		return client.CoreV1().Events("").List(callCtx, metav1.ListOptions{
			FieldSelector: "reason=OOMKilling",
		})
	})
	if err != nil {
		return nil, err
	}
	return findCSIOOMKillingEvents(events.Items, csiPodName, targetNode, since), nil
}

func collectNodeMemoryTelemetry(ctx context.Context, client clientset.Interface, targetNode, csiNamespace, csiPodName string) (wsMiB, rssMiB, cgroupPeakMiB float64) {
	diagCtx, cancel := context.WithTimeout(ctx, memorySoakDiagTimeout)
	defer cancel()

	const mib = 1024.0 * 1024.0

	if statsRaw, err := client.CoreV1().RESTClient().Get().
		Resource("nodes").
		Name(targetNode).
		SubResource("proxy").
		Suffix("stats/summary").
		DoRaw(diagCtx); err == nil {
		if wsBytes, rssBytes, found, parseErr := parseStatsSummaryMemory(statsRaw, csiNamespace, csiPodName, memorySoakCSIContainerName); parseErr == nil && found {
			wsMiB = float64(wsBytes) / mib
			rssMiB = float64(rssBytes) / mib
		}
	} else {
		framework.Logf("Non-gating diagnostics: failed to fetch /stats/summary for node %q: %v", targetNode, err)
	}

	if cadvisorRaw, err := client.CoreV1().RESTClient().Get().
		Resource("nodes").
		Name(targetNode).
		SubResource("proxy").
		Suffix("metrics/cadvisor").
		DoRaw(diagCtx); err == nil {
		if peakBytes, found := parseCadvisorMemoryPeak(cadvisorRaw, csiNamespace, csiPodName, memorySoakCSIContainerName); found {
			cgroupPeakMiB = float64(peakBytes) / mib
		}
	} else {
		framework.Logf("Non-gating diagnostics: failed to fetch /metrics/cadvisor for node %q: %v", targetNode, err)
	}

	return wsMiB, rssMiB, cgroupPeakMiB
}

func writeSoakDiagnostics(ctx context.Context, client clientset.Interface, artifactsDir string, summary *memorySoakSummary) {
	diagCtx, cancel := context.WithTimeout(ctx, memorySoakDiagTimeout)
	defer cancel()

	summary.EndTime = time.Now().UTC().Format(time.RFC3339)

	var currentPodName string
	var needPreviousLogs bool
	if summary.CSINamespace != "" && summary.TargetNode != "" {
		if pod, err := findActiveCSIPodOnNode(diagCtx, client, summary.CSINamespace, summary.TargetNode); err == nil {
			currentPodName = pod.Name
			summary.CSIPodName = pod.Name
			if cs, csErr := getCSIContainerStatus(pod); csErr == nil {
				finalSummary := &memorySoakCSIContainerSummary{
					Ready:        cs.Ready,
					RestartCount: cs.RestartCount,
					Running:      cs.State.Running != nil,
				}
				if cs.State.Terminated != nil {
					finalSummary.TerminatedReason = cs.State.Terminated.Reason
					finalSummary.TerminatedExitCode = cs.State.Terminated.ExitCode
				}
				if cs.LastTerminationState.Terminated != nil {
					finalSummary.LastTerminatedReason = cs.LastTerminationState.Terminated.Reason
					finalSummary.LastTerminatedExit = cs.LastTerminationState.Terminated.ExitCode
				}
				summary.FinalCSIContainer = finalSummary
				needPreviousLogs = cs.RestartCount > 0 || summary.Status != "passed"
			} else {
				needPreviousLogs = summary.Status != "passed"
			}
		} else {
			summary.DiagnosticWriteErrors = append(summary.DiagnosticWriteErrors, fmt.Sprintf("fetch final CSI pod: %v", err))
		}
	}
	if currentPodName == "" && summary.CSINamespace != "" && summary.CSIPodName != "" {
		currentPodName = summary.CSIPodName
		needPreviousLogs = summary.Status != "passed"
	}

	if err := os.MkdirAll(artifactsDir, 0o755); err != nil {
		framework.Logf("Failed to create artifacts directory %q: %v", artifactsDir, err)
		return
	}

	if currentPodName != "" {
		var logBuf bytes.Buffer
		tailLines := memorySoakLogTailLines
		if logs, err := client.CoreV1().Pods(summary.CSINamespace).GetLogs(currentPodName, &corev1.PodLogOptions{
			Container: memorySoakCSIContainerName,
			TailLines: &tailLines,
		}).DoRaw(diagCtx); err == nil {
			logBuf.WriteString(fmt.Sprintf("=== CURRENT LOGS (%s/%s container=%s) ===\n", summary.CSINamespace, currentPodName, memorySoakCSIContainerName))
			logBuf.Write(logs)
			logBuf.WriteByte('\n')
		} else {
			summary.DiagnosticWriteErrors = append(summary.DiagnosticWriteErrors, fmt.Sprintf("fetch current CSI logs: %v", err))
		}

		if needPreviousLogs {
			if prevLogs, err := client.CoreV1().Pods(summary.CSINamespace).GetLogs(currentPodName, &corev1.PodLogOptions{
				Container: memorySoakCSIContainerName,
				Previous:  true,
				TailLines: &tailLines,
			}).DoRaw(diagCtx); err == nil && len(prevLogs) > 0 {
				logBuf.WriteString(fmt.Sprintf("\n=== PREVIOUS LOGS (%s/%s container=%s) ===\n", summary.CSINamespace, currentPodName, memorySoakCSIContainerName))
				logBuf.Write(prevLogs)
				logBuf.WriteByte('\n')
			}
		}

		if logBuf.Len() > 0 {
			logPath := filepath.Join(artifactsDir, "csi_driver_logs.txt")
			//nolint:gosec // G306: $ARTIFACTS files must be readable by non-root Prow sidecar uploader.
			if err := os.WriteFile(logPath, logBuf.Bytes(), 0o644); err != nil {
				summary.DiagnosticWriteErrors = append(summary.DiagnosticWriteErrors, fmt.Sprintf("write %s: %v", logPath, err))
			} else {
				framework.Logf("Wrote CSI driver logs to %s (%d bytes)", logPath, logBuf.Len())
			}
		}
	}

	summaryBytes, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		framework.Logf("Failed to marshal memory soak summary JSON: %v", err)
		return
	}
	summaryPath := filepath.Join(artifactsDir, "memory_soak_summary.json")
	//nolint:gosec // G306: $ARTIFACTS files must be readable by non-root Prow sidecar uploader.
	if err := os.WriteFile(summaryPath, summaryBytes, 0o644); err != nil {
		framework.Logf("Failed to write %s: %v", summaryPath, err)
		return
	}
	framework.Logf("Wrote memory soak summary to %s", summaryPath)
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func truncateStrings(s []string, maxLen int) []string {
	if len(s) <= maxLen {
		return s
	}
	return append(append([]string{}, s[:maxLen]...), fmt.Sprintf("... (%d more)", len(s)-maxLen))
}
