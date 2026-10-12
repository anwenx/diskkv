package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errBuf bytes.Buffer
	code := run(args, &out, &errBuf)
	return code, out.String(), errBuf.String()
}

func TestCLIWorkflow(t *testing.T) {
	db := filepath.Join(t.TempDir(), "cli.db")

	if code, out, _ := runCLI(t, "-db", db, "set", "user:1", "alice"); code != 0 || out != "OK\n" {
		t.Fatalf("set: code=%d out=%q", code, out)
	}
	if code, out, _ := runCLI(t, "-db", db, "set", "user:2", "bob"); code != 0 || out != "OK\n" {
		t.Fatalf("set: code=%d out=%q", code, out)
	}

	if code, out, _ := runCLI(t, "-db", db, "get", "user:1"); code != 0 || out != "alice\n" {
		t.Fatalf("get: code=%d out=%q", code, out)
	}

	code, out, _ := runCLI(t, "-db", db, "scan", "user:")
	if code != 0 || out != "user:1\talice\nuser:2\tbob\n" {
		t.Fatalf("scan: code=%d out=%q", code, out)
	}

	if code, out, _ := runCLI(t, "-db", db, "delete", "user:1"); code != 0 || out != "OK\n" {
		t.Fatalf("delete: code=%d out=%q", code, out)
	}

	code, _, errOut := runCLI(t, "-db", db, "get", "user:1")
	if code != 1 || !strings.Contains(errOut, "key not found") {
		t.Fatalf("get missing: code=%d stderr=%q", code, errOut)
	}

	code, out, _ = runCLI(t, "-db", db, "stats")
	if code != 0 || !strings.Contains(out, "live_keys: 1\n") {
		t.Fatalf("stats: code=%d out=%q", code, out)
	}

	if code, out, _ := runCLI(t, "-db", db, "compact"); code != 0 || out != "Compacted\n" {
		t.Fatalf("compact: code=%d out=%q", code, out)
	}

	if code, out, _ := runCLI(t, "-db", db, "get", "user:2"); code != 0 || out != "bob\n" {
		t.Fatalf("get after compact: code=%d out=%q", code, out)
	}
}

func TestCLIUsageErrors(t *testing.T) {
	db := filepath.Join(t.TempDir(), "cli.db")

	for _, args := range [][]string{
		{},
		{"-db", db},
		{"bogus"},
		{"-db", db, "set", "only-key"},
		{"-db", db, "get"},
		{"-db", db, "stats", "extra"},
	} {
		if code, _, _ := runCLI(t, args...); code != 2 {
			t.Errorf("args %v: code=%d, want 2", args, code)
		}
	}
}
