# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- CASMCMS-8300: Rootfs passthrough now protects against additional empty strings.

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
### Changed
- Spelling corrections.

## [1.2.80]: Version released in CSM-1.2
- Spelling corrections.
- Fixes rootfs provisioner issues when no rootfs provisioner is used


