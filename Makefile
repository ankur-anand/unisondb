# Run all tests.
# set COVERAGE_DIR If not set
COVERAGE_DIR ?= .coverage
.PHONY: test
test:
	@echo "[go test] running unit tests and collecting coverage metrics"
	@-rm -r $(COVERAGE_DIR)
	@mkdir $(COVERAGE_DIR)
	@go test -v -race -covermode atomic -coverprofile $(COVERAGE_DIR)/combined.txt `go list ./... | grep -v tmp `

.PHONY: test_integration
test_integration:
	@echo "[go test] running integration tests and collecting coverage metrics"
	@-rm -r $(COVERAGE_DIR)
	@mkdir $(COVERAGE_DIR)
	@go test -v -tags integration_tests -race -covermode atomic -coverprofile $(COVERAGE_DIR)/combined.txt `go list ./... | grep -v tmp`

# get the html coverage
html-coverage:
	@go tool cover -html=$(COVERAGE_DIR)/combined.txt

# Run all lint
.PHONY: lint
lint: lint-check-deps
	@echo "[golangci-lint] linting sources"
	@golangci-lint run ./...

# Install the lint dependencies
.PHONY: lint-check-deps
lint-check-deps:
	@if [ -z `which golangci-lint` ]; then \
		echo "[go get] installing golangci-lint";\
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.61.0;\
	fi


