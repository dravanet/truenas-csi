# Makefile for TrueNAS-CSI

PROG = truenas-csi

GO = go

INSTALL = install

all: $(PROG)

$(PROG):
	CGO_ENABLED=0 $(GO) build -ldflags -s -o $(PROG) ./cmd/node/

install:
	$(INSTALL) -d $(DESTDIR)/usr/sbin
	$(INSTALL) -m 755 -t $(DESTDIR)/usr/sbin/ $(PROG)
