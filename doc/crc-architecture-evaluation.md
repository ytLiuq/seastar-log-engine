# CRC Architecture Evaluation

Date: 2026-05-02

## Context

Benchmark profiles consistently show `record_crc_enabled=true` carries a
~33% throughput penalty relative to baseline, stable across payload sizes
(256B / 1KB / 4KB). Five optimization attempts (zlib crc32, slicing-by-8,
copy+CRC fusion, special-char scanning, bulk field writes) each yielded
≤2% improvement. The bottleneck is structural: scanning every byte of
every payload for CRC is inherently expensive.

This document evaluates three architectural-level directions for reducing
CRC overhead without eliminating data integrity guarantees.

## Question 1: Must CRC cover the entire payload?

**Current state:** CRC32 covers the full record body (all structured fields
+ the sanitized payload). This means for a 4KB payload, CRC processes 4KB
of data per record.

**Option A: CRC covers metadata only (fields + fixed-size payload prefix)**

- CRC would cover: timestamp, level, shard_id, sequence, CRC field itself,
  and the first N bytes of payload (e.g., 256 bytes).
- The remaining payload suffix is not checksummed.
- Throughput gain: for 4KB payload with 256B prefix, CRC work drops ~94%.
- Risk: bit rot in the payload suffix is silently accepted.
- Assessment: **Rejected for general use.** The point of application-level
  CRC is to detect storage-level corruption, which is equally likely in any
  part of the payload. Partial coverage weakens the guarantee.

**Option B: CRC covers a hash of the payload instead of the payload itself**

- Instead of `CRC32(fields + payload)`, compute `CRC32(fields + XXH64(payload))`.
- The fast hash (XXH64 ~30 GB/s) scans the payload once; CRC32 only touches
  the hash output (8 bytes).
- Integrity: collision probability is XXH64 collision rate (~2^-64) — acceptable
  for storage corruption detection.
- Throughput estimate: XXH64 at ~30 GB/s would add ~0.13us for 4KB vs CRC32's
  current cost. Savings scale with payload size.
- Complexity: adds a dependency on xxhash or a hand-rolled fast hash.
- Assessment: **Promising.** Separates the "scan every byte" requirement from
  the "CRC32 framing" requirement. Worth prototyping.

**Option C: Per-block CRC with early exit**

- Split payload into fixed-size blocks (e.g., 512B), compute CRC per block,
  store concatenated CRCs in a trailer field.
- On read, validate blocks independently. A single bad block flags the record.
- Benefit: CRC computation can be vectorized over block boundaries. Recovery
  can potentially isolate which block is bad.
- Cost: record format changes significantly. Trailer parsing overhead.
- Assessment: **High complexity, low ROI.** The CRC computation itself is the
  bottleneck, not verification granularity.

**Recommendation:** Prototype Option B (hash-then-CRC). If XXH64 throughput
improves CRC-on performance by >20%, pursue it. Otherwise, accept the ~33%
penalty as the cost of integrity and focus on making CRC opt-in per record class.

## Question 2: Per-record-class validation

**Current state:** CRC is all-or-nothing at the engine config level
(`record_crc_enabled=true/false`). Every record either has CRC or none do.

**Proposal: CRC class system**

Define record classes with different integrity levels:

| Class | Name | CRC Coverage | Use Case |
|-------|------|-------------|----------|
| 0 | none | No CRC | High-throughput operational logs, debug output |
| 1 | header | CRC over metadata fields only | Metrics/events where payload is best-effort |
| 2 | full | CRC over entire record body | Audit logs, financial transactions, compliance |

Implementation:
- Add `record_crc_class` field to `EngineConfig` (default: 2 for backward compat).
- Add `LogMessage::crc_class` field (default: inherit from config).
- Encode CRC class into the record format (e.g., `crc=<value>` for class 2,
  `crc=h:<value>` for class 1, omitted for class 0).
- Reader validates according to the class:
  - Class 0: skip validation
  - Class 1: validate header CRC only, skip payload
  - Class 2: validate full CRC (current behavior)

**Throughput impact:**
- Class 0: baseline throughput (no CRC work at all).
- Class 1: near-baseline for large payloads (only ~100 bytes of metadata hashed).
- Class 2: current ~33% penalty.

**Migration path:** Add `crc_class` field to `LogMessage`. Default to class 2
for existing callers. Applications opt down to class 0 or 1 for high-throughput
non-critical logs. Query server reports per-class validation stats.

**Assessment: Recommended.** This is the lowest-risk approach. It preserves the
full CRC guarantee for records that need it while allowing high-throughput
workloads to reduce or skip CRC. Implementation is straightforward and doesn't
require algorithm changes.

## Question 3: Cheaper integrity alternatives

**Alternative 1: Kernel-level checksums (O_DIRECT integrity)**

- If the filesystem and storage stack already provide block-level checksums
  (e.g., ZFS, btrfs, dm-integrity), application-level CRC may be redundant.
- Trade-off: relies on specific storage configuration. Not portable.
- Assessment: Document as a deployment consideration but don't couple the
  engine to specific filesystems.

**Alternative 2: Write-then-verify**

- Instead of CRC, append a record length prefix. On read, verify that the
  next record starts at the expected byte offset. Mismatch → corruption.
- Benefit: near-zero CPU cost. Records are already length-delimited.
- Limitation: cannot detect within-record corruption that preserves length
  (e.g., a bit flip inside the payload). Catches truncation and partial writes.
- Assessment: **Too weak for audit use.** Length-only validation misses the
  most insidious corruption patterns. Not a replacement for CRC.

**Alternative 3: Periodic sampling**

- Only compute CRC for every Nth record or every record that crosses a
  filesystem block boundary.
- Reduces average CRC cost proportionally.
- Problem: non-deterministic coverage. An attacker or systematic error can
  target the non-CRCed records.
- Assessment: **Rejected.** Statistical integrity is not integrity.

**Alternative 4: SIMD-accelerated CRC (CRC32C via PCLMULQDQ)**

- Modern x86 CPUs support CRC32C (Castagnoli) via the `crc32` instruction
  and PCLMULQDQ for fast polynomial multiplication.
- Performance: hardware CRC32C can reach ~20-30 GB/s on modern CPUs, vs the
  current software slicing-by-8 at ~2-3 GB/s.
- Limitation: CRC32C uses a different polynomial than CRC32 (IEEE 802). This
  changes the CRC values but not the integrity guarantees. Requires Seastar
  to run on x86 with SSE4.2+. Breaks ARM compatibility for the fast path.
- Assessment: **Worth evaluating for x86-only deployments.** The potential
  speedup is an order of magnitude. Implementation complexity is moderate
  (conditionally compiled paths for x86 vs generic).

## Summary of Recommendations

| Priority | Direction | Expected Impact | Risk |
|----------|-----------|----------------|------|
| 1 (immediate) | Per-record-class CRC (class 0/1/2) | Up to baseline throughput for class 0 | Low: additive feature, backward-compatible |
| 2 (prototype) | Hash-then-CRC (XXH64 + CRC32) | 15-25% improvement for class 2 | Medium: new dependency, needs benchmark validation |
| 3 (evaluate) | Hardware CRC32C for x86 | 5-10x faster CRC computation | Medium: architecture-specific, breaks ARM parity |
| 4 (document) | Filesystem-level integrity as CRC alternative | N/A (deployment choice) | Low: documentation only |

## Decision Log

- **2026-05-02**: Evaluation written. Recommend starting with per-record-class
  CRC (Priority 1) as the lowest-risk, highest-impact change. Hash-then-CRC
  and hardware CRC32C should be prototyped before committing.
