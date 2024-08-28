#
# MIT License
#
# (C) Copyright 2019-2024 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# Dockerfile for Boot Orchestration Agent

FROM artifactory.algol60.net/csm-docker/stable/docker.io/library/alpine:3.17 as base
WORKDIR /app
ADD constraints.txt requirements.txt /app/
RUN --mount=type=secret,id=netrc,target=/root/.netrc \
    apk add --upgrade --no-cache apk-tools &&  \
	apk update && \
	apk add --no-cache linux-headers gcc g++ python3-dev py3-pip musl-dev libffi-dev openssl-dev git && \
	apk -U upgrade --no-cache && \
    pip3 list --format freeze && \
    pip3 install --no-cache-dir -U pip && \
    pip3 list --format freeze && \
    pip3 install --no-cache-dir -r requirements.txt  && \
    pip3 list --format freeze
COPY src/ /app/lib/

FROM base as install
COPY setup.py .version /app/lib/
RUN cd /app/lib && \
    pip3 install --no-cache-dir . && \
    pip3 list --format freeze && \
    rm -rf /app/*
COPY where_is_code.sh /app/

FROM base as testing
WORKDIR /app/
COPY docker_test_entry.sh .
RUN pip3 install --no-cache-dir -r /app/lib/cray/boa/test/test_requirements.txt && \
    pip3 list --format freeze
ENTRYPOINT [ "./docker_test_entry.sh" ]

FROM install as debug
RUN apk add --no-cache busybox-extras && \
    pip3 install --no-cache-dir rpdb && \
    pip3 list --format freeze
ENTRYPOINT [ "python3", "-m", "cray.boa" ]

FROM install as prod
USER 65534:65534
ENTRYPOINT [ "python3", "-m", "cray.boa" ]
