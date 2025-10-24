# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Bug Fixes

- Resolve schema mismatch for COUNT(*) and aggregation queries ([#32](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/32)) ([f4d22f3](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/f4d22f3920f1f67f2516ef875e8c0b958011b750))

### Documentation

- Updates CONTRIBUTING ([5a3d949](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/5a3d9490ca5d6bf49508b32025b5f5d0befa2c7e))

### Features

- Upgrade to DataFusion 50.2.0 ([#30](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/30)) ([6abdbc4](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/6abdbc44acd71a305549a91d605edcbc65a7c0fe))
- Implement DROP TABLE support for ClickHouse tables ([#33](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/33)) ([9870f5f](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/9870f5f3e85a0b36ba4701b523b11ce8df4079be))
- Comprehensive examples enhancement with DataFusion 50.2.0 and DROP TABLE support ([#34](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/34)) ([7af16c1](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/7af16c1f989b55452b5dc4e6ae81c37066738b43))

### Miscellaneous Tasks

- Creates separate security workflow and updates ci ([#18](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/18)) ([cd53f05](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/cd53f05afc872493d74815c3ef1a4ea589e41b4b))
- Updates release workflow, dry run ([#19](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/19)) ([184c72a](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/184c72ac71a642084a2d727503dce046f8af53a4))
- Patches typo in variable for release ([2a51c93](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/2a51c93ea83375958d03bea738689b6ce258248e))
- Upgrades datafusion-federation to 0.4.9 ([#23](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/23)) ([102c4e0](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/102c4e016c15dfe6271611cfa28129577f5e779a))
- Upgrades clickhouse-arrow 0.1.6 ([#24](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/24)) ([80e3a11](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/80e3a11e41e4d566fd71356a333f899b37e8ce73))
- Addresses minor lints, patches ci workflow ([#37](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/37)) ([e310554](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/e310554f2670c35b476b710ad04d9a0380c0db8f))

### Build

- Updates lock file ([a79637a](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/a79637a392336dbf1958dcc0fec89c52737404fe))

## [0.1.2] - 2025-08-13

### Bug Fixes

- Updates prelude to re-export more, minor cleanup to docs ([#16](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/16)) ([bb5bbb0](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/bb5bbb0560f956ce6df44fa086c6831b99a2d706))

### Miscellaneous Tasks

- Prepare release v0.1.2 ([#17](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/17)) ([3d8baf9](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/3d8baf99442ad12d2e40b5e1becc28e0d314b0dd))

## [0.1.1] - 2025-08-12

### Documentation

- Removes incorrect 'by default' around federation feature ([#9](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/9)) ([dda5ebb](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/dda5ebb4bc4198b2053efcb95cc95351ddd5affc))
- Updates README to use full version in docs ([#10](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/10)) ([31289b0](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/31289b0e0b9954bdc4ce0886d502d4fbc25ad122))
- Updates example in README ([#12](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/12)) ([142c23b](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/142c23b8f9164c38f3dbbbe6f44706824d62c0af))

### Miscellaneous Tasks

- Updates ci to patch change files step ([6d9736e](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/6d9736e500ad550ff1ade106f19038162acea0e9))
- Testing ci change with readme update ([#13](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/13)) ([4475ec9](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/4475ec9355a1a3c84af5b33bcc1d9cfa8c3b16c5))
- Prepare release v0.1.1 ([#14](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/14)) ([bff9e66](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/bff9e665cd34a9f162fec81f0f8cda40d01a363b))
- Updates justfile correct incorrect prepare-release step ([#15](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/15)) ([791ad70](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/791ad7096b71ac7eb4f124af42e40685ef1c3015))

### Build

- Updates datafusion-federation, reconfigures CI workflow ([#11](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/11)) ([b658979](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/b65897940a83a608c81995dfd72124420d65e87d))

## [0.1.0] - 2025-08-12

### Bug Fixes

- Use crates.io datafusion-federation for publishing ([#7](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/7)) ([0a55875](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/0a55875602567ad2c6cf52a7d1b3a75262244635))
- Add required description field for crates.io ([8745559](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/87455590ab47e10ee6458a5586dd878bd89914d2))

### Documentation

- Updates readme ([b3feec9](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/b3feec91a9bcaa0a9a7b4ccdd97111683f2c9a29))
- Adds badges ([e2ecb5e](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/e2ecb5e1598969260dea94a360de98d063533aa9))
- Adds link to clickhouse-arrow in README ([542ae45](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/542ae45881c3feed35d1b4ca05c6285b89c42f2d))
- Add example ([#4](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/4)) ([e08c707](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/e08c7077a441755014973dfe1df39dc6713a09f5))
- Updates docs ([#5](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/5)) ([f1d89dc](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/f1d89dcc3bd6a224deedf1b223ec6aa1ab8d0aa4))

### Miscellaneous Tasks

- Updates ci workflow to login to docker ([#3](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/3)) ([79e87d8](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/79e87d8b374a7a23a4a09a46be359ebfdd809dff))
- Prepare release v0.1.0 ([#6](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/6)) ([db70d45](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/db70d451283f8cd1d1e64ba6c6d05337e654036b))

### Refactor

- Gpatterson/analyzer-refactor ([#1](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/1)) ([5935e53](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/5935e53d62f92ef7d2e507ce7648b3292f94d8ad))

### Testing

- Asserts failure on insert e2e test due to upstream bug, datafusion ([#8](https://github.com/georgeleepatterson/clickhouse-datafusion/issues/8)) ([340f2b7](https://github.com/georgeleepatterson/clickhouse-datafusion/commit/340f2b7ce0b7638abe8afb2f34d24e106a914efe))


