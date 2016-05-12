FROM registry.opensource.zalan.do/stups/openjdk:8u77-b03-1-20

RUN mkdir -p /app
RUN mkdir -p /app/conf/logging
COPY target/kairosdb-1.3-SNAPSHOT-distribution.tar.gz /app/kairosdb.tar.gz
RUN cd /app && tar xvf kairosdb.tar.gz
RUN rm /app/kairosdb.tar.gz

EXPOSE 8080

CMD ["/app/bin/kairosdb.sh", "run"]
