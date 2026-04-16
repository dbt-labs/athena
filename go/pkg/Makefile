GO_BUILD := go build
RM := rm -f

ifeq ($(shell go env GOOS),linux)
	SUFFIX=so
else ifeq ($(shell go env GOOS),windows)
	SUFFIX=dll
	RM=del /Q 2>nul ||
else
	SUFFIX=dylib
endif

.PHONY: all clean test

all: libadbc_driver_athena.$(SUFFIX)

# Run unit tests (pure-Go helpers) and CGo integration tests against the built .so.
test: libadbc_driver_athena.$(SUFFIX)
	cd .. && go test -tags driverlib -v ./pkg/athena/
ifeq ($(shell go env GOOS),linux)
	LD_LIBRARY_PATH=. go run ./shimtest/
else ifeq ($(shell go env GOOS),darwin)
	DYLD_LIBRARY_PATH=. go run ./shimtest/
endif

SRCS := $(wildcard athena/*.go athena/*.c athena/*.h)

libadbc_driver_athena.$(SUFFIX): $(SRCS) ../go.mod ../go.sum
	$(GO_BUILD) -buildvcs=true -tags driverlib,netgo \
		-o $@ -buildmode=c-shared \
		./athena
	$(RM) $(basename $@).h

clean:
	$(RM) libadbc_driver_athena.$(SUFFIX) libadbc_driver_athena.h
