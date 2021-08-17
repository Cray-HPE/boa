# Cray Boot Orchestration Service

This is the Cray Boot Orchestration Service (BOS).
It provides a service that orchestrates the booting, rebooting, and
shutdown of compute nodes.

## Architecture

The architecture for BOS is described in Confluence at
https://connect.us.cray.com/confluence/pages/viewpage.action?pageId=133562640.

## Requirements

## Build the Docker image
docker build -t boa .

## Build a debug version of the Docker image that has rpdb and busybox in it
docker build -t boa --target debug .

## Debugging hints
Building a debug image is easy enough to do. Use rpdb to debug issues. It 
provides good visibility into otherwise inscrutable problems.

Creating a BOA Docker image tagged with your user ID is an easy way to put your
content on the system and later remove it without worrying about overwriting
the latest BOA Docker image.
The normal image is named cray/cray-boa. Simply name yours <userid>/cray-boa.
Then, you can alter the configmap boa-config where the image is identified.
Change the 'cray' below to your <userid>.
data:
  boa_image: bis.local:5000/cray/cray-boa:latest
As a final step, locate and delete the BOS pod. This forces it to restart
and pick up the new configmap with the new image.
```
kubectl -n services get pods | grep bos
kubectl -n services delete pod <bos-pod-id>
```

To clean up, remember to revert your changes in the boa-config map and 
restart the BOS pod.

You can cause node boots to time out faster by adding the following 
environment variables to the configmap boa-job-template. These variables
do not appear in the configmap by default. Their default values are shown
below.
env:
  - name: "NODE_STATE_CHECK_NUMBER_OF_RETRIES"
    value: "120"
  - name: "NODE_STATE_CHECK_SLEEP_INTERVAL"
    value: "5"

NODE_STATE_CHECK_NUMBER_OF_RETRIES -- BOA will check on the expected state of nodes this many times before
                                      giving up. You can crank this down to a very low number to make 
                                      BOA time-out quickly.
NODE_STATE_CHECK_SLEEP_INTERVAL -- This is how long BOA will sleep between checks. You can crank this down to a very low number to make 
                                      BOA time-out quickly.

Note: Changing NODE_STATE_CHECK_SLEEP_INTERVAL will make the process happen more quickly than NODE_STATE_CHECK_SLEEP_INTERVAL.

## TESTING

### Unit Tests
Here is how to run the unit test.
First, build the testing Docker image.
docker build . --target testing -t artifactory.algol60.net/csm-docker/unstable/cray-boa:testing
Second, create a results directory
mkdir -p results
Third, run the test image, placing the results into the local results directory.
docker run -d --mount type=bind,source="$(pwd)"/results,target=/results artifactory.algol60.net/csm-docker/unstable/cray-boa:testing
Fourth, check the results directory for results of the unit test run.
less ./results/pytests.out
Fix any test failures and repeat until all tests pass.

## Build Helpers
This repo uses some build helpers from the 
[cms-meta-tools](https://github.com/Cray-HPE/cms-meta-tools) repo. See that repo for more details.

## Local Builds
If you wish to perform a local build, you will first need to clone or copy the contents of the
cms-meta-tools repo to `./cms_meta_tools` in the same directory as the `Makefile`. When building
on github, the cloneCMSMetaTools() function clones the cms-meta-tools repo into that directory.

For a local build, you will also need to manually write the .version, .docker_version (if this repo
builds a docker image), and .chart_version (if this repo builds a helm chart) files. When building
on github, this is done by the setVersionFiles() function.

## Versioning
The version of this repo is generated dynamically at build time by running the version.py script in 
cms-meta-tools. The version is included near the very beginning of the github build output. 

In order to make it easier to go from an artifact back to the source code that produced that artifact,
a text file named gitInfo.txt is added to Docker images built from this repo. For Docker images,
it can be found in the /app folder. This file contains the branch from which it was built and the most
recent commits to that branch. 

For helm charts, a few annotation metadata fields are appended which contain similar information.

For RPMs, a changelog entry is added with similar information.

## New Release Branches
When making a new release branch:
    * Be sure to set the .x and .y files to the desired major and minor version number for this repo for this release. 
    * If an update_external_versions.conf file exists in this repo, be sure to update that as well, if needed.

## Copyright and License
This project is copyrighted by Hewlett Packard Enterprise Development LP and is under the MIT
license. See the [LICENSE](LICENSE) file for details.

When making any modifications to a file that has a Cray/HPE copyright header, that header
must be updated to include the current year.

When creating any new files in this repo, if they contain source code, they must have
the HPE copyright and license text in their header, unless the file is covered under
someone else's copyright/license (in which case that should be in the header). For this
purpose, source code files include Dockerfiles, Ansible files, RPM spec files, and shell
scripts. It does **not** include Jenkinsfiles, OpenAPI/Swagger specs, or READMEs.

When in doubt, provided the file is not covered under someone else's copyright or license, then
it does not hurt to add ours to the header.
