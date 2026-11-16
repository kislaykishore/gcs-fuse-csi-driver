/*
Copyright 2025 Google LLC

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

package utils

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/util/version"
)

// TODO(amacaskill): Add test to presubmit by adding to make unit-test target.
func TestGCSFuseBranch(t *testing.T) {
	t.Parallel()
	t.Run("Testing GCSFuseBranch", func(t *testing.T) {
		t.Parallel()
		testCases := []struct {
			name              string
			gcsfuseVersionStr string
			expectedBranch    string
			expectedVersion   *version.Version
		}{
			{
				name:              "should return master for GCSFuse built from HEAD",
				gcsfuseVersionStr: "0.0.1-gcsfuse-git-master-abcdef",
				expectedBranch:    "master",
				expectedVersion:   nil,
			},
			{
				name:              "should return branch for not valid GCSFuse version v3.3.0-gke.1",
				gcsfuseVersionStr: "3.3.0-gke.1",
				expectedBranch:    "v3.3.0_release",
				expectedVersion:   version.MustParseSemantic("3.3.0-gke.1"),
			},
		}

		for _, tc := range testCases {
			t.Logf("test case: %s", tc.name)
			v, branch := GCSFuseBranch(tc.gcsfuseVersionStr)
			if branch != tc.expectedBranch {
				t.Errorf("For branch, got value %q, but expected %q", branch, tc.expectedBranch)
			}

			if tc.expectedVersion == nil {
				if v != nil {
					t.Errorf("For version, got %v, but expected nil", v)
				}
			} else if v == nil {
				t.Errorf("For version, got %v, but expected %v", v, tc.expectedVersion)
			} else if v.String() != tc.expectedVersion.String() {
				t.Errorf("For version, got %q, but expected %q", v.String(), tc.expectedVersion.String())
			}
		}
	})
}

func TestParseConfigFlags(t *testing.T) {
	t.Parallel()

	t.Run("Testing ParseConfigFlags", func(t *testing.T) {
		t.Parallel()

		const testProject = "test-project-id"
		os.Setenv(ProjectEnvVar, testProject)

		testCases := []struct {
			name           string
			flagStr        string
			expectedParsed ParsedConfig
		}{
			{
				name:    "should parse standard file cache flags correctly",
				flagStr: "--file-cache-max-size-mb=9,--file-cache-cache-file-for-range-read=true,--metadata-cache-ttl-secs=10,--o=ro,--log-severity=trace",
				expectedParsed: ParsedConfig{
					FileCacheCapacity: "9Mi",
					ReadOnly:          true,
					LogSeverity:       "trace",
					MountOptions: []string{
						"file-cache-cache-file-for-range-read=true",
						"metadata-cache-ttl-secs=10",
					},
				},
			},
			{
				name:    "should fallback to exact default values when flags are empty",
				flagStr: "",
				expectedParsed: ParsedConfig{
					FileCacheCapacity: "-1Mi",
					ReadOnly:          false,
					LogSeverity:       "info",
					MountOptions:      []string{},
				},
			},
			{
				name:    "should correctly trim space around flags",
				flagStr: "  --file-cache-max-size-mb=100  ,  --metadata-cache-ttl-secs=0  ",
				expectedParsed: ParsedConfig{
					FileCacheCapacity: "100Mi",
					ReadOnly:          false,
					LogSeverity:       "info",
					MountOptions: []string{
						"metadata-cache-ttl-secs=0",
					},
				},
			},
			{
				name:    "should translate configuration flags correctly",
				flagStr: "--log-file=/tmp/log,--file-cache-enable-parallel-downloads,--file-cache-enable-o-direct=false,--enable-kernel-reader=true,--log-format=text,--cache-dir=/tmp/cache",
				expectedParsed: ParsedConfig{
					FileCacheCapacity: "-1Mi",
					ReadOnly:          false,
					LogFilePath:       "/tmp/log",
					LogSeverity:       "info",
					CacheDir:          "/tmp/cache",
					MountOptions: []string{
						"logging:file-path:/tmp/log",
						"file-cache-enable-parallel-downloads",
						"file-cache-enable-o-direct=false",
						"enable-kernel-reader=true",
						"logging:format:text",
					},
				},
			},
			{
				name:    "should swallow disallowed flags with no manual handling via default continue",
				flagStr: "--temp-dir=/tmp/foo,--prometheus-port=8080,--foreground",
				expectedParsed: ParsedConfig{
					FileCacheCapacity: "-1Mi",
					ReadOnly:          false,
					LogSeverity:       "info",
					MountOptions:      []string{},
				},
			},
			{
				name:    "should correctly translate and pass log-format but swallow others",
				flagStr: "--log-format=json,--config-file=/etc/gcsfuse.yaml",
				expectedParsed: ParsedConfig{
					FileCacheCapacity: "-1Mi",
					ReadOnly:          false,
					LogSeverity:       "info",
					MountOptions: []string{
						"logging:format:json",
					},
				},
			},
			{
				name:    "should correctly parse multiple space-separated flags",
				flagStr: "--stat-cache-ttl=0 --client-protocol=grpc",
				expectedParsed: ParsedConfig{
					FileCacheCapacity: "-1Mi",
					ReadOnly:          false,
					LogSeverity:       "info",
					MountOptions: []string{
						"stat-cache-ttl=0",
						"client-protocol=grpc",
					},
				},
			},
			{
				name:    "should override billing-project with env var",
				flagStr: "--billing-project=hardcoded-project",
				expectedParsed: ParsedConfig{
					FileCacheCapacity: "-1Mi",
					ReadOnly:          false,
					LogSeverity:       "info",
					BillingProject:    testProject,
					MountOptions: []string{
						"billing-project=" + testProject,
					},
				},
			},
		}

		for _, tc := range testCases {
			t.Logf("test case: %s", tc.name)
			got := ParseConfigFlags(tc.flagStr)

			if diff := cmp.Diff(tc.expectedParsed, got); diff != "" {
				t.Errorf("ParseConfigFlags(%q) mismatch (-want +got):\n%s", tc.flagStr, diff)
			}
		}
	})
}

func TestGenerateTestSkipMemorySoak(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name              string
		ginkgoFocus       string
		expectSoakSkipped bool
	}{
		{
			name:              "normal full e2e run skips memory soak test",
			ginkgoFocus:       "",
			expectSoakSkipped: true,
		},
		{
			name:              "non-soak focused run skips memory soak test",
			ginkgoFocus:       "metrics",
			expectSoakSkipped: true,
		},
		{
			name:              "performance focused run skips memory soak test",
			ginkgoFocus:       "performance",
			expectSoakSkipped: true,
		},
		{
			name:              "generic memory focus skips memory soak test",
			ginkgoFocus:       "memory",
			expectSoakSkipped: true,
		},
		{
			name:              "memory soak focused run does not skip memory soak test",
			ginkgoFocus:       "should.succeed.in.memory.soak.test",
			expectSoakSkipped: false,
		},
		{
			name:              "partial memory.soak focus does not skip memory soak test",
			ginkgoFocus:       "memory.soak",
			expectSoakSkipped: false,
		},
		{
			name:              "hyphenated memory-soak focus does not skip memory soak test",
			ginkgoFocus:       "memory-soak",
			expectSoakSkipped: false,
		},
		{
			name:              "space-separated memory soak focus does not skip memory soak test",
			ginkgoFocus:       "memory soak",
			expectSoakSkipped: false,
		},
		{
			name:              "full plain spec name focus does not skip memory soak test",
			ginkgoFocus:       "should succeed in memory soak test",
			expectSoakSkipped: false,
		},
		{
			name:              "regex wildcard memory.*soak focus does not skip memory soak test",
			ginkgoFocus:       "memory.*soak",
			expectSoakSkipped: false,
		},
		{
			name:              "regex whitespace memory\\ssoak focus does not skip memory soak test",
			ginkgoFocus:       `memory\ssoak`,
			expectSoakSkipped: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			params := &TestParameters{
				GinkgoFocus:                  tc.ginkgoFocus,
				SupportsNativeSidecar:        true,
				SupportMachineTypeAutoconfig: true,
				GkeClusterVersion:            "1.35.1-gke.100",
			}
			gotSkip := generateTestSkip(params)
			isSkipped := strings.Contains(gotSkip, "should.succeed.in.memory.soak.test")
			if isSkipped != tc.expectSoakSkipped {
				t.Errorf("generateTestSkip(focus=%q) = %q: contains memory soak skip = %t, want %t",
					tc.ginkgoFocus, gotSkip, isSkipped, tc.expectSoakSkipped)
			}
		})
	}
}

func TestApplyMemorySoakDefaults(t *testing.T) {
	t.Parallel()

	for _, focus := range []string{
		"should.succeed.in.memory.soak.test",
		"memory-soak",
		"memory soak",
		"should succeed in memory soak test",
	} {
		t.Run("applies defaults when unset and focus="+focus, func(t *testing.T) {
			t.Parallel()
			params := &TestParameters{
				GinkgoFocus:         focus,
				GinkgoSkip:          "should.succeed.in.memory.soak.test|oidc",
				GinkgoProcs:         "40",
				GinkgoFlakeAttempts: "2",
				GinkgoTimeout:       "4h",
				NumNodes:            9,
				NodeMachineType:     "n2-standard-8",
				SoakDuration:        "4h",
			}
			ApplyMemorySoakDefaults(params, map[string]bool{})

			if params.GinkgoSkip != "" {
				t.Errorf("GinkgoSkip = %q, want empty", params.GinkgoSkip)
			}
			if params.GinkgoProcs != "1" {
				t.Errorf("GinkgoProcs = %q, want 1", params.GinkgoProcs)
			}
			if params.GinkgoFlakeAttempts != "1" {
				t.Errorf("GinkgoFlakeAttempts = %q, want 1", params.GinkgoFlakeAttempts)
			}
			if params.NumNodes != 1 {
				t.Errorf("NumNodes = %d, want 1", params.NumNodes)
			}
			if params.NodeMachineType != "n2-standard-32" {
				t.Errorf("NodeMachineType = %q, want n2-standard-32", params.NodeMachineType)
			}
			if params.GinkgoTimeout != "5h" {
				t.Errorf("GinkgoTimeout = %q, want 5h", params.GinkgoTimeout)
			}
		})
	}

	t.Run("respects explicit node/machine/larger timeout overrides", func(t *testing.T) {
		t.Parallel()
		params := &TestParameters{
			GinkgoFocus:         "should.succeed.in.memory.soak.test",
			GinkgoProcs:         "40",
			GinkgoFlakeAttempts: "2",
			GinkgoTimeout:       "8h",
			NumNodes:            2,
			NodeMachineType:     "n2-standard-64",
			SoakDuration:        "4h",
		}
		explicit := map[string]bool{
			"number-nodes":      true,
			"node-machine-type": true,
			"ginkgo-timeout":    true,
		}
		ApplyMemorySoakDefaults(params, explicit)

		if params.NumNodes != 2 {
			t.Errorf("NumNodes = %d, want 2", params.NumNodes)
		}
		if params.NodeMachineType != "n2-standard-64" {
			t.Errorf("NodeMachineType = %q, want n2-standard-64", params.NodeMachineType)
		}
		if params.GinkgoTimeout != "8h" {
			t.Errorf("GinkgoTimeout = %q, want 8h", params.GinkgoTimeout)
		}
	})

	t.Run("bumps explicit timeout if shorter than soakDuration+1h", func(t *testing.T) {
		t.Parallel()
		params := &TestParameters{
			GinkgoFocus:   "should.succeed.in.memory.soak.test",
			GinkgoTimeout: "4h",
			SoakDuration:  "6h",
		}
		ApplyMemorySoakDefaults(params, map[string]bool{"ginkgo-timeout": true})
		if params.GinkgoTimeout != "7h" {
			t.Errorf("GinkgoTimeout = %q, want 7h", params.GinkgoTimeout)
		}
	})

	for _, focus := range []string{"metrics", "memory", "performance", ""} {
		t.Run("no-op when focus does not target memory soak: "+focus, func(t *testing.T) {
			t.Parallel()
			params := &TestParameters{
				GinkgoFocus:         focus,
				GinkgoSkip:          "oidc",
				GinkgoProcs:         "40",
				GinkgoFlakeAttempts: "2",
				GinkgoTimeout:       "4h",
				NumNodes:            9,
				NodeMachineType:     "n2-standard-8",
			}
			ApplyMemorySoakDefaults(params, map[string]bool{})
			if params.NumNodes != 9 || params.NodeMachineType != "n2-standard-8" || params.GinkgoProcs != "40" || params.GinkgoSkip != "oidc" {
				t.Errorf("unexpected mutation on non-soak params: %+v", params)
			}
		})
	}
}

func TestComputeMaintenanceExclusionDuration(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		ginkgoTimeout string
		want          time.Duration
	}{
		{
			name:          "4h soak with 5h ginkgo timeout gets 7h window",
			ginkgoTimeout: "5h",
			want:          7 * time.Hour,
		},
		{
			name:          "standard 4h ginkgo timeout gets 6h window",
			ginkgoTimeout: "4h",
			want:          6 * time.Hour,
		},
		{
			name:          "minute-formatted timeout adds 2h buffer",
			ginkgoTimeout: "180m",
			want:          5 * time.Hour,
		},
		{
			name:          "empty timeout falls back to 4h base plus 2h buffer",
			ginkgoTimeout: "",
			want:          6 * time.Hour,
		},
		{
			name:          "invalid timeout falls back to 4h base plus 2h buffer",
			ginkgoTimeout: "not-a-duration",
			want:          6 * time.Hour,
		},
		{
			name:          "non-positive timeout falls back to 4h base plus 2h buffer",
			ginkgoTimeout: "-1h",
			want:          6 * time.Hour,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := computeMaintenanceExclusionDuration(tc.ginkgoTimeout); got != tc.want {
				t.Errorf("computeMaintenanceExclusionDuration(%q) = %s, want %s", tc.ginkgoTimeout, got, tc.want)
			}
		})
	}
}

func TestValidateMemorySoakParameters(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		params     *TestParameters
		wantErrSub string
	}{
		{
			name: "non-soak focus skips soak parameter validation",
			params: &TestParameters{
				GinkgoFocus:          "metrics",
				SoakPodCount:         0,
				SoakDuration:         "invalid",
				SoakScrapeInterval:   "0s",
				CSIDriverMemoryLimit: "bad",
			},
		},
		{
			name: "generic memory focus skips soak parameter validation",
			params: &TestParameters{
				GinkgoFocus:  "memory",
				SoakPodCount: 0,
			},
		},
		{
			name: "valid memory soak parameters pass",
			params: &TestParameters{
				GinkgoFocus:          "memory-soak",
				SoakPodCount:         80,
				SoakDuration:         "4h",
				SoakScrapeInterval:   "60s",
				CSIDriverMemoryLimit: "100Mi",
			},
		},
		{
			name: "empty CSI driver memory limit disables override cleanly",
			params: &TestParameters{
				GinkgoFocus:          "should succeed in memory soak test",
				SoakPodCount:         10,
				SoakDuration:         "10m",
				SoakScrapeInterval:   "30s",
				CSIDriverMemoryLimit: "",
			},
		},
		{
			name: "zero soak pod count rejected",
			params: &TestParameters{
				GinkgoFocus:        "memory-soak",
				SoakPodCount:       0,
				SoakDuration:       "4h",
				SoakScrapeInterval: "60s",
			},
			wantErrSub: "--soak-pod-count",
		},
		{
			name: "negative soak pod count rejected",
			params: &TestParameters{
				GinkgoFocus:        "memory soak",
				SoakPodCount:       -4,
				SoakDuration:       "4h",
				SoakScrapeInterval: "60s",
			},
			wantErrSub: "--soak-pod-count",
		},
		{
			name: "invalid soak duration syntax rejected",
			params: &TestParameters{
				GinkgoFocus:        "memory-soak",
				SoakPodCount:       80,
				SoakDuration:       "bogus",
				SoakScrapeInterval: "60s",
			},
			wantErrSub: "--soak-duration",
		},
		{
			name: "non-positive soak duration rejected",
			params: &TestParameters{
				GinkgoFocus:        "memory-soak",
				SoakPodCount:       80,
				SoakDuration:       "0s",
				SoakScrapeInterval: "60s",
			},
			wantErrSub: "--soak-duration",
		},
		{
			name: "invalid soak scrape interval rejected",
			params: &TestParameters{
				GinkgoFocus:        "memory-soak",
				SoakPodCount:       80,
				SoakDuration:       "4h",
				SoakScrapeInterval: "-5s",
			},
			wantErrSub: "--soak-scrape-interval",
		},
		{
			name: "invalid CSI driver memory limit syntax rejected",
			params: &TestParameters{
				GinkgoFocus:          "memory-soak",
				SoakPodCount:         80,
				SoakDuration:         "4h",
				SoakScrapeInterval:   "60s",
				CSIDriverMemoryLimit: "not-a-qty",
			},
			wantErrSub: "--csi-driver-memory-limit",
		},
		{
			name: "zero CSI driver memory limit rejected",
			params: &TestParameters{
				GinkgoFocus:          "memory-soak",
				SoakPodCount:         80,
				SoakDuration:         "4h",
				SoakScrapeInterval:   "60s",
				CSIDriverMemoryLimit: "0",
			},
			wantErrSub: "--csi-driver-memory-limit",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateMemorySoakParameters(tc.params)
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

	t.Run("Handle fails fast before cluster provisioning on invalid soak params", func(t *testing.T) {
		t.Parallel()
		err := Handle(&TestParameters{
			GinkgoFocus:            "memory-soak",
			SoakPodCount:           0,
			ManageClusterLifecycle: true,
			GkeGcloudCommand:       "/nonexistent/gcloud",
		})
		if err == nil || !strings.Contains(err.Error(), "--soak-pod-count") {
			t.Fatalf("Handle() error = %v, want fast failure mentioning --soak-pod-count", err)
		}
	})
}

func TestBuildMemorySoakGinkgoArgs(t *testing.T) {
	t.Parallel()

	got := buildMemorySoakGinkgoArgs(&TestParameters{
		CSIDriverMemoryLimit: "",
		SoakPodCount:         0,
		SoakDuration:         "10m",
		SoakScrapeInterval:   "30s",
	})
	want := []string{
		"--csi-driver-memory-limit=",
		"--soak-pod-count=0",
		"--soak-duration=10m",
		"--soak-scrape-interval=30s",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("buildMemorySoakGinkgoArgs mismatch (-want +got):\n%s", diff)
	}

	gotNeg := buildMemorySoakGinkgoArgs(&TestParameters{
		CSIDriverMemoryLimit: "100Mi",
		SoakPodCount:         -5,
	})
	wantNeg := []string{
		"--csi-driver-memory-limit=100Mi",
		"--soak-pod-count=-5",
	}
	if diff := cmp.Diff(wantNeg, gotNeg); diff != "" {
		t.Errorf("buildMemorySoakGinkgoArgs negative count mismatch (-want +got):\n%s", diff)
	}
}
