// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

// Package e2e holds end-to-end tests that exercise localcache across a real
// process boundary via its on-disk snapshot.
//
// Every test file in this package carries the e2e build tag, so the default
// unit gate (go test ./...) compiles this file and reports "no test files"
// instead of failing on a package with no buildable sources. Run the suite with:
//
//	go test -race -tags e2e ./pkg/base/localcache/e2e/...
package e2e
