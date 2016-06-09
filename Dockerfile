FROM registry.opensource.zalan.do/stups/openjdk:8-24

EXPOSE 8080

RUN mkdir -p /app/conf/logging
WORKDIR /app

ADD target/kairosdb-1.3-SNAPSHOT-distribution.tar.gz /app/

COPY logback.xml /app/conf/logging
COPY conf/kairosdb.properties /app/conf/kairosdb.properties

COPY target/scm-source.json /

CMD ["/app/bin/kairosdb.sh", "run"]
