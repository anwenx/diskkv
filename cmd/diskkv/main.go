package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/anwenx/diskkv/internal/storage"
)

func usage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  diskkv set <key> <value>")
	fmt.Fprintln(os.Stderr, "  diskkv get <key>")
	fmt.Fprintln(os.Stderr, "  diskkv delete <key>")
	fmt.Fprintln(os.Stderr, "  diskkv compact")
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	need := map[string]int{"set": 4, "get": 3, "delete": 3, "compact": 2}
	if expected, ok := need[cmd]; !ok || len(os.Args) != expected {
		usage()
		os.Exit(2)
	}

	s, err := storage.Open("diskkv.db")
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer s.Close()

	switch cmd {
	case "set":
		err = s.Set(os.Args[2], os.Args[3])
		if err == nil {
			fmt.Println("OK")
		}
	case "get":
		var value string
		value, err = s.Get(os.Args[2])
		if err == nil {
			fmt.Println(value)
		}
	case "delete":
		err = s.Delete(os.Args[2])
		if err == nil {
			fmt.Println("OK")
		}
	case "compact":
		err = s.Compact()
		if err == nil {
			fmt.Println("Compacted")
		}
	}
	if errors.Is(err, storage.ErrNotFound) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
