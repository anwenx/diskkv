package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/anwenx/diskkv/internal/storage"
)

func usage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  diskkv [-db path] set <key> <value>")
	fmt.Fprintln(w, "  diskkv [-db path] get <key>")
	fmt.Fprintln(w, "  diskkv [-db path] delete <key>")
	fmt.Fprintln(w, "  diskkv [-db path] scan <prefix>   (use \"\" for all keys)")
	fmt.Fprintln(w, "  diskkv [-db path] stats")
	fmt.Fprintln(w, "  diskkv [-db path] compact")
	fmt.Fprintln(w, "The default database file is diskkv.db in the current directory.")
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes one CLI invocation and returns the process exit code:
// 0 on success, 1 on operation failure, 2 on usage errors.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("diskkv", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "diskkv.db", "path to the database file")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	rest := fs.Args()
	if len(rest) == 0 {
		usage(stderr)
		return 2
	}

	need := map[string]int{"set": 3, "get": 2, "delete": 2, "scan": 2, "stats": 1, "compact": 1}
	if expected, ok := need[rest[0]]; !ok || len(rest) != expected {
		usage(stderr)
		return 2
	}

	s, err := storage.Open(*dbPath)
	if err != nil {
		fmt.Fprintln(stderr, "open:", err)
		return 1
	}

	code := execute(s, rest, stdout, stderr)

	if err := s.Close(); err != nil {
		fmt.Fprintln(stderr, "close:", err)
		if code == 0 {
			code = 1
		}
	}
	return code
}

func execute(s *storage.Store, args []string, stdout, stderr io.Writer) int {
	var err error

	switch args[0] {
	case "set":
		err = s.Set(args[1], args[2])
		if err == nil {
			fmt.Fprintln(stdout, "OK")
		}
	case "get":
		var value string
		value, err = s.Get(args[1])
		if err == nil {
			fmt.Fprintln(stdout, value)
		}
	case "delete":
		err = s.Delete(args[1])
		if err == nil {
			fmt.Fprintln(stdout, "OK")
		}
	case "scan":
		var kvs []storage.KeyValue
		kvs, err = s.ScanPrefix(args[1])
		if err == nil {
			for _, kv := range kvs {
				fmt.Fprintf(stdout, "%s\t%s\n", kv.Key, kv.Value)
			}
		}
	case "stats":
		var st storage.Stats
		st, err = s.Stats()
		if err == nil {
			printStats(stdout, st)
		}
	case "compact":
		err = s.Compact()
		if err == nil {
			fmt.Fprintln(stdout, "Compacted")
		}
	}

	if errors.Is(err, storage.ErrNotFound) {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

func printStats(w io.Writer, st storage.Stats) {
	last := "never"
	if !st.LastCompaction.IsZero() {
		last = st.LastCompaction.Format(time.RFC3339)
	}
	fmt.Fprintf(w, "live_keys: %d\n", st.LiveKeys)
	fmt.Fprintf(w, "file_size_bytes: %d\n", st.FileSize)
	fmt.Fprintf(w, "live_bytes: %d\n", st.LiveBytes)
	fmt.Fprintf(w, "stale_bytes: %d\n", st.StaleBytes)
	fmt.Fprintf(w, "sets: %d\n", st.Sets)
	fmt.Fprintf(w, "gets: %d\n", st.Gets)
	fmt.Fprintf(w, "deletes: %d\n", st.Deletes)
	fmt.Fprintf(w, "bytes_written: %d\n", st.BytesWritten)
	fmt.Fprintf(w, "compactions: %d\n", st.Compactions)
	fmt.Fprintf(w, "last_compaction: %s\n", last)
	fmt.Fprintf(w, "compaction_recommended: %t\n", st.CompactionRecommended)
}
