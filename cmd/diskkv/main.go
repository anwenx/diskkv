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
}

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(2)
	}

	store, err := storage.Open("diskkv.db")
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer store.Close()

	switch os.Args[1] {
	case "set":
		if len(os.Args) != 4 {
			usage()
			os.Exit(2)
		}
		err = store.Set(os.Args[2], os.Args[3])
		if err == nil {
			fmt.Println("OK")
		}

	case "get":
		if len(os.Args) != 3 {
			usage()
			os.Exit(2)
		}
		var value string
		value, err = store.Get(os.Args[2])
		if err == nil {
			fmt.Println(value)
		}

	case "delete":
		if len(os.Args) != 3 {
			usage()
			os.Exit(2)
		}
		err = store.Delete(os.Args[2])
		if err == nil {
			fmt.Println("OK")
		}

	default:
		usage()
		os.Exit(2)
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
