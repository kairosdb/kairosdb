## THIS EXPECTS TO BE INVOKED AFTER A TAR BUILD.
## USE 'java -cp tools/tablesaw-1.2.6.jar  make docker-image'
## TO BUILD.

FROM azul/zulu-openjdk-debian:8u202

ARG VERSION

RUN adduser --no-create-home --disabled-password kairosdb

ADD build/kairosdb-${VERSION}.tar /opt/

ADD ["https://hq-stash.corp.proofpoint.com/projects/PULSE/repos/pulse-kairosdb-plugins/raw/prometheus-adapter/prometheus-adapter-1.0.jar?at=refs%2Fheads%2Fmaster", "/opt/kairosdb/lib/prometheus-adapter/"]

RUN chown -R kairosdb /opt/kairosdb

WORKDIR /opt/kairosdb

## KairosDB Configuration Environment Variables
ENV KAIROSDB_SERVICE_TELNET=<disabled>

CMD ["/opt/kairosdb/bin/kairosdb.sh", "run"]

# Kairos API telnet and jetty ports
EXPOSE 4242 8083

USER kairosdb

LABEL maintainer="brianhks1+kairos@gmail.com" \
      org.label-schema.schema-version="1.0" \
      org.label-schema.name="kairosdb" \
      org.label-schema.description="KairosDB is a time series database that stores numeric values along\
 with key/value tags to a nosql data store.  Currently supported\
 backends are Cassandra and H2.  An H2 implementation is provided\
 for development work." \
org.label-schema.docker.dockerfile="/Dockerfile"