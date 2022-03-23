FROM alpine:3.15

LABEL org.opencontainers.image.authors "Richard Kojedzinszky <richard@kojedz.in>"
LABEL org.opencontainers.image.source https://github.com/dravanet/truenas-csi

RUN apk --no-cache add util-linux nfs-utils e2fsprogs-extra xfsprogs-extra

COPY truenas-csi /usr/local/bin/

COPY assets/ /

CMD ["/usr/local/bin/truenas-csi"]
