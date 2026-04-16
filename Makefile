.PHONY: test test-unit test-integration vet

GO_DIR := ./go

# Load .env if present (KEY=VALUE format, no `export` prefix needed)
-include .env
export

# Run only the unit tests (no AWS credentials required)
test-unit:
	cd $(GO_DIR) && go test ./... -count=1

# Run integration tests against real Athena.
# Requires: ATHENA_S3_STAGING_DIR, and AWS credentials in the environment.
test-integration:
	cd $(GO_DIR) && ADBC_ATHENA_TESTS=1 go test ./... -run TestIntegration -v -count=1

# Default: unit tests only
test: test-unit

vet:
	cd $(GO_DIR) && go vet ./...
