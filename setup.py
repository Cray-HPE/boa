# setup.py for cray-boa
# Copyright 2019, Cray Inc. All rights reserved.
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open(".version", "r") as fh:
    version_str = fh.read()

setuptools.setup(
    name="cray-boa",
    version=version_str,
    author="Cray Inc.",
    author_email="sps@cray.com",
    description="Boot Orchestration Agent",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://stash.us.cray.com/projects/SCMS/repos/boa/browse",
    packages=['cray.boa', 'cray.boa.rootfs', 'cray.boa.smd', 'cray.boa.log', 'cray.boa.bootimagemetadata'],
    keywords="cray boot orchestration agent",
    classifiers=(
        "Programming Language :: Python :: 3.7",
        "License :: Other/Proprietary License",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Systems Administration",
    ),
)
