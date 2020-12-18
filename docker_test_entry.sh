#!/bin/sh
# Copyright 2019-2020 Hewlett Packard Enterprise Development LP
set -e
set -o pipefail

mkdir -p /results

# pytest equivalent of the above
# The above nosetests can be removed once pytest duplicates
# all of the needed functionality.
pytest --rootdir=/app/lib/cray/boa/test 2>&1 | tee /results/pytests.out
 