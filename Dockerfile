# Dockerfile for Boot Orchestration Agent
# Copyright 2019-2021 Hewlett Packard Enterprise Development LP
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
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# (MIT License)

FROM artifactory.algol60.net/docker.io/alpine:3.13 as base
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
