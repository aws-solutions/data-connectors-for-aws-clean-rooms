# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2023-01-31

### Added

- All files, initial version

## [1.0.1] - 2023-02-06

### Changed

- [Inbound bucket prefix incorrectly applied for automatic trigger in AppFlow stack](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/11)

## [1.1.0] - 2023-03-02

- [Add Email notification for failing to automatic trigger DataBrew transform job](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/6)
- [Handle errors caused by`empty-file-object` file placed in the inbound bucket during installation](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/19)
- [Optimize automatic trigger DataBrew transform job workflow](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/7)

## [1.1.1] - 2023-04-18

- [Set s3 object ownership in order to maintain compatibility with s3 access logging [#44](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/44)]

## [1.2.0] - 2023-05-02

- [cdk-solution-helper-py compatibility with cdk-lib versions greater than 2.61.0 [#8](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/8)]
- [Additional asserts statements and enhancements to unit test [#40](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/40)]
- [Removed empty test file from project [#39](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/39)]
- [Clean up .viperlightignore file [#38](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/38)]
- [SOLUTION_VERSION in cdk.json updated [37](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/37)]
- [Assert in unit test that expiry time (ttl) is calculated correctly [36](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/36)]
- [Instructions in README.md file now behave as expected for running unit tests [#35](https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/issues/35)] 

## [1.2.1] - 2023-08-14

- Upgrade ``requests`` library to version 2.31.0 to fix vulnerability reported by Dependabot
- Modify Access logging bucket object ownership to Bucket owner preferred
- Add SQS-managed encryption to SQS queue in the solution
- Add new constant for duplicated IAM message string
