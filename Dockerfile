FROM alpine:3.20

ARG TARGETARCH

LABEL org.opencontainers.image.authors "Richard Kojedzinszky <richard@kojedz.in>"
LABEL org.opencontainers.image.source https://github.com/dravanet/truenas-csi

RUN apk --no-cache add util-linux nfs-utils e2fsprogs-extra xfsprogs-extra

COPY truenas-csi.${TARGETARCH} /usr/local/bin/truenas-csi

COPY assets/ /

CMD ["/usr/local/bin/truenas-csi"]
