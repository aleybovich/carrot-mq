.PHONY: test test-race clean-test-cache

# Run the full test suite (root integration tests + internal unit tests).
# -count=1 bypasses Go's test cache so the run is always real; 1m timeout
# because the suite spins up real servers and is not instant.
test:
	go test -count=1 -timeout 1m ./...

# Run the full test suite under the race detector. -count=1 matters here: a
# cached pass could otherwise mask an intermittent race. Longer timeout since
# the race detector is slower.
test-race:
	go test -race -count=1 -timeout 2m ./...

clean-test-cache:
	go clean -testcache
