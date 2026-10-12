.PHONY: build test race vet fmt bench check clean

build:
	go build -o diskkv ./cmd/diskkv

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

bench:
	go test -bench . -benchmem -run '^$$' ./internal/storage

check: vet test race

clean:
	rm -f diskkv
