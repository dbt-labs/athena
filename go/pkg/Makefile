GO_BUILD := go build
RM := rm -f

ifeq ($(shell go env GOOS),linux)
	SUFFIX=so
else ifeq ($(shell go env GOOS),windows)
	SUFFIX=dll
	RM=del
else
	SUFFIX=dylib
endif

.PHONY: all clean

all: libadbc_driver_athena.$(SUFFIX)

libadbc_driver_athena.$(SUFFIX): athena/ ../go.mod ../go.sum
	$(GO_BUILD) -buildvcs=true -tags driverlib,netgo \
		-o $@ -buildmode=c-shared \
		./athena
	$(RM) $(basename $@).h

clean:
	$(RM) libadbc_driver_athena.$(SUFFIX) libadbc_driver_athena.h
