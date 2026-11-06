# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- A reliable command's resend timeout now follows C ENet (`protocol.c`): one round trip plus the smaller of another round trip and four times the round-trip variance (the variance taken as at least 1 ms), capped at `timeout_maximum / 5` and multiplied by the send attempts so far (up to `timeout_limit`). It is no longer floored at `timeout_minimum` (5 s by default), so a lost reliable packet is resent after about one round trip instead of holding its channel for 5 s. The disconnect rule is unchanged.

## [0.1.1] - 2026-06-14

### Added

- CI workflow check for README.md.

### Changed

- Pinned dependency versions to exact patch versions for reproducible builds.
- Removed platform-dependent `socket2` feature for cross-platform compatibility ([#3](https://github.com/hgaiser/tokio-enet/pull/3)).

### Fixed

- `socket::Socket` no longer uses `socket2::Socket::only_v6()` which is not available on all platforms.

## [0.1.0] - 2026-03-19

### Added

- Initial implementation of the ENet protocol in pure Rust.
- `Host` for creating ENet servers and clients.
- `Peer` for managing connections to remote peers.
- `Packet` for sending and receiving data.
- `Channel` for ordered, unreliable messaging.
- `Compressor` type for protocol-level compression.
- `Socket` abstraction over UDP sockets.
- Protocol codec for encoding/decoding ENet packets.
- Connection test suite.
