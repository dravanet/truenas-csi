FROM debian:buster-slim

LABEL org.opencontainers.image.authors "Richard Kojedzinszky <richard@kojedz.in>"
LABEL org.opencontainers.image.source https://github.com/dravanet/truenas-csi

RUN apt-get update && \
    apt-get -y install open-iscsi nfs-common e2fsprogs xfsprogs && \
    apt-get clean

COPY truenas-csi /

CMD ["/truenas-csi"]
