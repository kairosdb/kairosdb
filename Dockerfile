FROM eclipse-temurin:17.0.17_10-jre-alpine-3.23

RUN apk add bash

ADD ./build/kairosdb-*.tar /opt

EXPOSE 8080 4242

WORKDIR /opt/kairosdb

ENTRYPOINT [ "/bin/bash" ]
CMD [ "bin/kairosdb.sh", "run" ]