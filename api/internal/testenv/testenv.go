// Package testenv decides what a missing test dependency means.
//
// The store, queue, and rate-limit tests need a real Postgres or Redis. Locally
// it is reasonable to skip them — not everyone has both running for every
// change. In CI it is not: a suite that skips its integration tests reports a
// confident green while verifying almost nothing, which is worse than a red
// build because it is believed.
//
// So the same missing variable is a skip on a developer's machine and a hard
// failure in CI.
package testenv

import (
	"os"
	"testing"
)

// InCI reports whether the suite is running in a continuous integration
// environment. Every major CI provider sets CI=true.
func InCI() bool {
	return os.Getenv("CI") != ""
}

// RequireOrSkip fails the test in CI when an environment variable is missing,
// and skips it otherwise. It returns the variable's value.
func RequireOrSkip(t *testing.T, name, why string) string {
	t.Helper()

	value := os.Getenv(name)
	if value != "" {
		return value
	}

	if InCI() {
		t.Fatalf("%s is not set. %s\n"+
			"CI must exercise these tests against real infrastructure; skipping them here "+
			"would report a passing build that verified nothing.", name, why)
	}

	t.Skipf("set %s to run this test. %s", name, why)
	return ""
}
