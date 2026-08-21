//go:build e2e

// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

// Package e2e exercises localcache the way a service does: a real process that
// fills the cache under concurrent load, persists it to disk on Destroy, and a
// second process that loads that file back and serves from it.
//
// It is excluded from the default unit gate by the e2e build tag; run it with:
//
//	cd CubeMaster && go test -race -tags e2e ./pkg/base/localcache/e2e/...
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/localcache"
)

const (
	roleEnv  = "LOCALCACHE_E2E_ROLE"
	fileEnv  = "LOCALCACHE_E2E_FILE"
	keyCount = 64
	// Bounded so saveFile has real concurrent put() traffic to walk past
	// without the cache growing large enough to make the gob encode dominate.
	churnPerWorker = 256
)

func cacheConfig(file string, expired time.Duration) *localcache.LocalCacheConfig {
	return &localcache.LocalCacheConfig{
		LowCacheSize:       1000000,
		HighCacheSize:      2000000,
		Expired:            expired,
		AsyncRefreshBefore: time.Millisecond,
		MaxAsyncRefreshNum: 100,
		ExpiredUse:         true,
		OpenCacheFile:      true,
		LoadFileName:       file,
	}
}

// TestMain lets this binary re-exec itself as the writer or reader process, so
// the persisted file genuinely crosses a process boundary.
func TestMain(m *testing.M) {
	switch os.Getenv(roleEnv) {
	case "writer":
		os.Exit(runWriter(os.Getenv(fileEnv)))
	case "reader":
		os.Exit(runReader(os.Getenv(fileEnv)))
	default:
		os.Exit(m.Run())
	}
}

// runWriter fills the cache under concurrent readers and refreshes, then
// Destroys it — which is the path that snapshots every entry to disk while
// async refreshes may still be in flight.
func runWriter(file string) int {
	var loads int64
	cache := localcache.NewCache("e2e-writer",
		func(ctx context.Context, key string) (interface{}, bool, error) {
			atomic.AddInt64(&loads, 1)
			return "value-of-" + key, true, nil
		},
		cacheConfig(file, time.Hour))

	ctx := context.Background()
	// The keys the reader will assert on. Their persisted Expired is long, so
	// they are still valid when the second process loads them.
	for i := 0; i < keyCount; i++ {
		if _, _, err := cache.Get(ctx, fmt.Sprintf("k%d", i)); err != nil {
			fmt.Fprintln(os.Stderr, "writer seed failed:", err)
			return 1
		}
	}

	// Keep genuine put() traffic in flight across Destroy: every one of these
	// keys is a miss, so each drives loadAndRefresh -> put while saveFile walks
	// the same list.
	var wg sync.WaitGroup
	started := make(chan struct{})
	var once sync.Once
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < churnPerWorker; i++ {
				if i == churnPerWorker/8 {
					once.Do(func() { close(started) })
				}
				if _, _, err := cache.Get(ctx, fmt.Sprintf("churn-%d-%d", r, i)); err != nil {
					return
				}
			}
		}(r)
	}

	<-started
	cache.Destroy()
	wg.Wait()

	fmt.Printf("writer loads=%d\n", atomic.LoadInt64(&loads))
	return 0
}

// runReader starts a fresh cache over the persisted file with a loader that
// refuses to run, so every hit must be served from what the writer saved.
func runReader(file string) int {
	cache := localcache.NewCache("e2e-reader",
		func(ctx context.Context, key string) (interface{}, bool, error) {
			return nil, false, fmt.Errorf("loader must not run for %s", key)
		},
		cacheConfig(file, time.Hour))
	defer cache.Destroy()

	ctx := context.Background()
	served := 0
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("k%d", i)
		v, found, err := cache.Get(ctx, key)
		if err != nil || !found {
			continue
		}
		s, ok := v.(string)
		if !ok {
			fmt.Fprintf(os.Stderr, "reader: %s holds %T, not a string — torn value\n", key, v)
			return 1
		}
		if s != "value-of-"+key {
			fmt.Fprintf(os.Stderr, "reader: %s = %q, want value-of-%s\n", key, s, key)
			return 1
		}
		served++
	}
	fmt.Printf("reader served=%d\n", served)
	return 0
}

func reexec(t *testing.T, role, file string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run", "TestPersistedCacheSurvivesAProcessBoundary")
	cmd.Env = append(os.Environ(), roleEnv+"="+role, fileEnv+"="+file)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s process failed: %v\n%s", role, err, out)
	}
	return string(out)
}

// TestPersistedCacheSurvivesAProcessBoundary is the end-to-end guard for the
// torn-interface and saveFile races: the writer Destroys while refreshes are in
// flight, and a separate process must read back well-typed, correct values.
func TestPersistedCacheSurvivesAProcessBoundary(t *testing.T) {
	if os.Getenv(roleEnv) != "" {
		return // re-exec'd child; TestMain already dispatched
	}

	file := filepath.Join(t.TempDir(), "cache.gob")

	writerOut := reexec(t, "writer", file)
	if _, ok := scanReported(writerOut, "writer loads="); !ok {
		t.Fatalf("writer did not report:\n%s", tail(writerOut, 5))
	}
	info, err := os.Stat(file)
	if err != nil {
		t.Fatalf("Destroy did not persist the cache file: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("Destroy wrote an empty cache file")
	}

	readerOut := reexec(t, "reader", file)
	served, ok := scanReported(readerOut, "reader served=")
	if !ok {
		t.Fatalf("reader did not report a count:\n%s", tail(readerOut, 5))
	}
	if served == 0 {
		t.Fatalf("a second process loaded the persisted file but served nothing:\n%s", readerOut)
	}
	loads, _ := scanReported(writerOut, "writer loads=")
	t.Logf("writer -> %d loader calls, %d bytes persisted", loads, info.Size())
	t.Logf("reader -> served %d/%d keys from the persisted file", served, keyCount)
}

// scanReported finds the marker line the child printed among the cache's own
// JSON log output and returns the integer that follows it.
func scanReported(out, marker string) (int, bool) {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		idx := strings.Index(line, marker)
		if idx < 0 {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(line[idx+len(marker):], "%d", &n); err == nil {
			return n, true
		}
	}
	return 0, false
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
