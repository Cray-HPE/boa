# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [1.4.4] - 2024-04-01
### Changed
- Disabled concurrent Jenkins builds on same branch/commit
- Added build timeout to avoid hung builds
### Fixed
- Removed nonexistent argument from docstring of `graceful_shutdown` function
  in `capmcclient.py`.
- CASMCMS-8274: Treat CAPMC node lock errors as immediate failures, rather than waiting to eventually
  fail due to timeout.

## [1.4.3] - 2023-07-18
### Dependencies
- Bump `PyYAML` from 6.0 to 6.0.1 to avoid build issue caused by https://github.com/yaml/pyyaml/issues/601

## [1.4.2] - 2023-05-19
### Changed
- Update message to use --template-name instead of --template-uuid in CLI command.

## [1.4.1] - 2023-05-18
### Changed
- Revert setup.py from Python 3.11 to 3.10
- Pin Alpine minor version in Dockerfile to Alpine 3.17

## [1.4.0] - 2023-05-18
### Changed
- CASMCMS-8300: Rootfs passthrough now protects against additional empty strings.
- Update setup.py from Python 3.6 to 3.11

## [1.3.2] - 2023-04-07
### Changed
- Allow provisioning of rootfs helperless bootsets, backport/casmcms-8300.

## [1.3.1] - 2022-12-20
### Added
- Add Artifactory authentication to Jenkinsfile

## [1.3.0] - 2022-08-11
### Changed
- Updated deprecation warnings for use of nid based calls to capmc
- Switch preflight check URI for capmc
- Pin to later version of Alpine. Update Python 3 dependencies.
- Allow BOA to avoid populating root=<flag> values when no provider, or rootfs passthrough parameters are used
- Initial implementation @jsollom-hpe

## [1.2.80]: Version released in CSM-1.2
### Changed
- Spelling corrections.
- Fixes rootfs provisioner issues when no rootfs provisioner is used
