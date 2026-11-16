# Rust HTTP port: implementation and validation record

Base: `89362ce9c0a22c6fc655f5aa3aa0f7ba43e4930d`, branch `rust_port`.
Both origin branches were checked on 2026-11-16. Original checkout was clean.
Work checkout: `D:\projekty_codex\nuviotv`. The analysis checkout is unchanged.

## Baseline

The local modified Media3 AARs remain authoritative. Their SHA-256 inventory is
in `native/streaming-engine/aar-baseline.json`. No decoder, extractor, torrent,
MPV, AFR/QMS or sample queue implementation is replaced.

Tooling found: JDK 21.0.13+9, Android SDK 36, NDK 29.0.14206865. Rust is
installed project-locally (no system PATH changes). Integrations can build with
empty optional API credentials. No APK credentials are recovered.

Baseline JVM command:
```
gradlew :app:testFullDebugUnitTest --tests '*ParallelRange*' --tests '*AutoSpeedup*' --tests '*PlaybackPrewarm*' --tests '*ConnectionLedger*' --tests '*HostSpeeds*'
```

Hardware results and A/B improvements must only be recorded after actual runs.
No Android TV test device or selected test media is supplied with this task.

## Repeatable measurements

`scripts/rust_range_server.py` serves a deterministic virtual file larger than
4 GiB without holding it in memory. Set `--size`, `--delay-ms`, `--truncate`,
`--status`, `--ignore-range` and `--port` to reproduce failures. Requests and
actually sent bytes are printed as JSON. Bind defaults to loopback; for device
testing use adb reverse or an explicitly selected interface. No VPS is used.

For A/B use identical release builds, budgets, connection caps, SHA-256 of real
MKV/MP4, cache state and network profile. Record 30 alternating runs per profile:
TTFF, seek p50/p95, rebuffer count/time, process CPU, PSS/Java/native memory,
downloaded/useful/discarded bytes, cancellation time and peak live requests.
Six-hour soak and 1000 seek/reopen cycles remain hardware acceptance gates.
