# Makefile for TrueNAS-CSI

PROG = truenas-csi

GO = go

BUILD_CPUS ?= $(shell getconf _NPROCESSORS_ONLN)

INSTALL = install

all: $(PROG)

$(PROG):
	CGO_ENABLED=0 $(GO) build -p $(BUILD_CPUS) -ldflags -s -o $(PROG) ./cmd/node/

install:
	$(INSTALL) -d $(DESTDIR)/usr/sbin
	$(INSTALL) -m 755 -t $(DESTDIR)/usr/sbin/ $(PROG)
