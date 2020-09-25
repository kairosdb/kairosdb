ARG KAIROSDB_VERSION=1.3.0-0.1beta

FROM openjdk:8u242-jdk-slim-buster as build
ARG KAIROSDB_VERSION

WORKDIR /home/kairosdb
ADD . /home/kairosdb/git

RUN cd git && \
    export CLASSPATH=tools/tablesaw-1.2.8.jar && \
    java make package

RUN tar -xzvf git/build/kairosdb-${KAIROSDB_VERSION}.tar.gz

FROM openjdk:8u242-jdk-slim-buster
ARG KAIROSDB_VERSION
ENV KAIROSDB_HOME=/opt/kairosdb-${KAIROSDB_VERSION}
ENV CLASSPATH=${KAIROSDB_HOME}/lib/*

COPY --from=build /home/kairosdb/kairosdb /opt/kairosdb-${KAIROSDB_VERSION}

RUN ln -s ${KAIROSDB_HOME}/conf /etc/kairosdb && \
    echo 'export PATH=${KAIROSDB_HOME}/bin:${PATH}' >> /root/.bashrc

EXPOSE 8080 4242

WORKDIR /opt/kairosdb-${KAIROSDB_VERSION}/bin
ENTRYPOINT . ~/.bashrc && kairosdb.sh run