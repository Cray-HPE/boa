# Dockerfile for Boot Orchestration Agent
# Copyright 2019, Cray Inc. All rights reserved.

FROM dtr.dev.cray.com/baseos/alpine:3.12.0 as base
WORKDIR /app
ADD constraints.txt requirements.txt /app/
RUN apk add --no-cache linux-headers gcc g++ python3-dev py3-pip musl-dev libffi-dev openssl-dev git && \
    PIP_INDEX_URL=http://dst.us.cray.com/dstpiprepo/simple \
    PIP_TRUSTED_HOST=dst.us.cray.com \
    pip3 install --no-cache-dir -U pip && \
    pip3 install --no-cache-dir -r requirements.txt
COPY setup.py .version src/ /app/lib/
RUN cd /app/lib && pip3 install --no-cache-dir .
ENTRYPOINT [ "python3", "-m", "cray.boa" ]

FROM base as debug
RUN apk add --no-cache busybox-extras && \
    pip3 install --no-cache-dir rpdb
