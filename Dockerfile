# Dockerfile for Boot Orchestration Agent
# Copyright 2019-2021 Hewlett Packard Enterprise Development LP

FROM dtr.dev.cray.com/baseos/alpine:3.12.0 as base
WORKDIR /app
ADD constraints.txt requirements.txt /app/
RUN apk add --no-cache linux-headers gcc g++ python3-dev py3-pip musl-dev libffi-dev openssl-dev git && \
    pip3 install --no-cache-dir -U pip && \
    pip3 install --no-cache-dir -r requirements.txt
COPY src/ /app/lib/

FROM base as install 
COPY setup.py .version /app/lib/
RUN cd /app/lib && pip3 install --no-cache-dir . && \
    rm -rf /app/*
COPY where_is_code.sh /app/

FROM base as testing
WORKDIR /app/
COPY docker_test_entry.sh .
RUN pip3 install --no-cache-dir -r /app/lib/cray/boa/test/test_requirements.txt
ENTRYPOINT [ "./docker_test_entry.sh" ]

FROM install as debug
RUN apk add --no-cache busybox-extras && \
    pip3 install --no-cache-dir rpdb
ENTRYPOINT [ "python3", "-m", "cray.boa" ]

FROM install as prod
ENTRYPOINT [ "python3", "-m", "cray.boa" ]
