FROM registry.opensource.zalan.do/stups/openjdk:latest

EXPOSE 8080

RUN mkdir -p /app/conf/logging
WORKDIR /app

ADD target/kairosdb-1.3-SNAPSHOT-distribution.tar.gz /app/

COPY logback.xml /app/conf/logging
COPY conf/kairosdb.properties /app/conf/kairosdb.properties
COPY lib/jolokia-jvm-1.6.0-agent.jar /app/lib/ 

CMD ["/app/bin/kairosdb.sh", "run"]
