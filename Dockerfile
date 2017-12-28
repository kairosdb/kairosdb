FROM registry.opensource.zalan.do/stups/openjdk:latest

EXPOSE 8080

RUN mkdir -p /app/conf/logging
WORKDIR /app

ADD target/kairosdb-1.3-SNAPSHOT-distribution.tar.gz /app/

COPY logback.xml /app/conf/logging
COPY conf/kairosdb.properties /app/conf/kairosdb.properties

CMD ["/app/bin/kairosdb.sh", "run"]
