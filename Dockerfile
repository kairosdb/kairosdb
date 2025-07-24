FROM alpine:3.22

RUN apk add openjdk11
RUN apk add bash

ADD ./build/kairosdb-*.tar /opt

EXPOSE 8080 4242

WORKDIR /opt/kairosdb

ENTRYPOINT [ "/bin/bash" ]
CMD [ "bin/kairosdb.sh", "run" ]