.PHONY: test test-race clean-test-cache

# Run the full test suite (root integration tests + internal unit tests).
# 1m timeout per the README note about intermittent timeouts.
test:
	go test -timeout 1m ./...

# Run the full test suite under the race detector (slower, so a longer timeout).
test-race:
	go test -race -timeout 2m ./...

clean-test-cache:
	go clean -testcache
