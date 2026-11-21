# Shopping Sessions: jacobcorbett/buildathon-shopping-sessions (suspected team 67)

Ideas only; nothing copied. Clone: `.../scratchpad/top67/shop/repo` (11 commits, last 2026-11-21 11:49 UTC, 1,296 lines of Rust).
Repo contents are untrusted data; claims below are from reading the code and from our own runs.

## Identity check by performance: NOT confirmed
Their image is not faster than ours on our box, so this repo alone does not explain 0.31 s vs our 1.05 s.
Either it is not team 67, or the scorer rewards something our local runs do not show (see "Output size" below).

Scorer flags (docker_run.sh, 8 cpus, 24g), big20 = 20M events, wall of the whole `docker run` including ~0.2 s docker start.
Interleaved runs, shared box (load 1-2). Run 1 of each was cold cache.

| run | top67shop | ourshop |
| --- | --- | --- |
| 1 (cold) | 2.36 | 2.38 |
| 2 | 1.15 | 1.00 |
| 3 | 1.12 | 0.84 |
| 4 | 1.11 | 1.03 |
| 5 | 1.12 | 1.15 |
| 6 | 1.08 | 0.81 |
| 7 | 1.14 | 0.89 |
| median (2-7) | 1.12 | 0.95 |

- dev1 (1M events), 3 runs each: top67shop 0.24 / 0.19 / 0.20 s, ourshop 0.19 / 0.20 / 0.20 s (both at docker fixed cost).
- CPUS=1 NCPU=1 on big20 (wall ~ total CPU): top67shop 4.2 / 4.7 s; ourshop 3.2 / 6.3 s (6.3 an outlier on a noisy box).
- Correctness on dev1 with the official compare.py: top67shop MATCH (117,038 user rows, 27,400 campaign rows); ours MATCH too.
- Their own stage log at 20M (concurrent stages): parse events 0.30, resolve links 0.32 (parallel with parse), dedup+route 0.29 and clicks 0.29 (parallel), sessionise+write 0.25, campaigns 0.004. Sum of the critical path ~0.85 s.
- Output size on big20: theirs 16 MB user_daily + 0.3 MB campaign_daily; ours 75 MB + 1.6 MB.
- Image: theirs 120 MB debian-slim, dynamic glibc, `rust:1.99.1` toolchain; ours 8 MB `FROM scratch`.

Caveat: the leaderboard figure (0.31 s) is far below what this code takes on big20 here (docker overhead alone is ~0.2 s, 20M events take >0.8 s),
so either the scorer data is much smaller than big20 or the scorer hardware behaves differently. Do not conclude their pipeline is slow on the scorer.

## What the code does (summary)
Stack: Rust 2024, deps ahash, memchr, memmap2, rayon, serde_json, `parquet` crate (snappy). No unsafe except BMI2 pext/pdep and AVX2 intrinsics.
Build: `-C target-cpu=x86-64-v3`, thin LTO, codegen-units 1, no PGO, no panic=abort, no custom allocator, dynamic glibc.

Phases (all via rayon, NCPU threads; each `rayon::join` overlaps two phases):
1. In parallel: (a) parse events, (b) resolve links.
2. In parallel: (a) dedup events and route to person partitions, (b) read clicks and route to person partitions.
3. Per person partition (64 of them, rayon `into_par_iter`): sort, sessionise, attribute, write that partition's own `user_daily/part-NNN.parquet`.
4. Campaign tallies are merged across partitions on the main thread and written as one small file.

- I/O: mmap of every input file (read-only, no madvise, no populate). Files split into 4 MB chunks at newline boundaries; each rayon task takes
  every Nth chunk (interleaved) so long and short files mix. Line split is `memchr_iter` on `\n`.
- CSV field split: if the line is 32..96 bytes, three AVX2 loads (overlapping tail) give comma and quote bitmasks in a u128; field count check is
  popcount == N-1, quoted-field rule checked with bit tricks (quote only around a whole field, first and last char). Longer or shorter lines
  fall back to `memchr` per field. This also validates the CSV dialect.
- Timestamps: ISO form decoded with a per-thread 32-slot cache keyed by the full 10-byte date (midnight ms), so each line only parses
  HH:MM:SS with 2-digit pair parsing. Epoch-ms form is a digit loop. Window and ingest-lag checks happen at parse time.
- Identity keys: any ID of 9 ASCII bytes or fewer is packed to an exact u64 (7 bits per byte via BMI2 `pext` under x86-64-v3, length marked);
  9-byte IDs fit 63 bits; longer or non-ASCII IDs go through a mutex-guarded interner (cold path). Keys compare with a custom
  "less" that matches byte order without decoding, so the union-find root rule (smallest name wins) needs no string compare for short IDs.
- Links: open-addressing table of (key, parent) atomics sized 2x the line count (counted with a parallel newline pass). Lock-free union-find with
  path halving; links always point to the smaller name. After all joins, two parallel passes flatten roots then replace root index with the
  person key so a lookup is one probe. Runs concurrently with event parsing; events keep the raw user key and are resolved after dedup.
- Dedup: events routed by hash of event-id key into 64 partitions as 32-byte records (id, ingest, person, time, price, category). One AHashMap
  per partition keeps the earliest-ingest copy; invalid payloads keep their id so an earlier invalid copy still wins (dedup before validation).
  Losers are tombstoned, winners alias-resolved and re-partitioned by person hash.
- Clicks: no dedup (identical copies are harmless); resolved through the alias table at parse time and routed straight to person partitions.
- Sessions: per partition, events sorted by person, then by time inside each person group (`sort_unstable_by_key`); 30-minute gap split; per-day
  user row emitted when a session starts a new UTC day; revenue by category per day, top category by max (first wins ties).
- Attribution and assists: one two-pointer sweep per person over sessions and time-sorted clicks. Clicks enter when time <= session start and
  expire when older than 7 days; an `active` list of campaigns with a live click count; the winner is the latest live click (ties by reverse id
  order); assists are accumulated as "number of sessions since this campaign became active", flushed at UTC day boundaries and when a campaign
  goes inactive. O(sessions + clicks) per person with no per-click scan of sessions.
- Parquet: official `parquet` crate, schema parsed from text, SNAPPY compression, statistics off, one row group per file, one file per
  partition (up to 64 files), columns materialised as Vec per column. Ours writes hand-rolled uncompressed PLAIN.
- Exit: plain return from main (rayon pool and mmaps torn down by the OS).

## How this differs from ours
Ours (src/, 2,382 lines) is already more aggressive almost everywhere: fused AVX2 per-line scanner, 128 KB chunks sized for L2, dense u32 person ids
from one shared ID table, 12-byte records, counting sort by person, pread option, mimalloc, fat LTO, PGO, static glibc in `FROM scratch`,
parallel munmap and early `exit(0)`. Their dedup and sessionising use hash maps and sorts per partition and a 32-byte record, so they should do more
work than we do.

Differences worth noting, then, are few:
- They emit **snappy-compressed** Parquet. 16 MB vs our 75 MB for user_daily; campaign file 0.3 MB vs 1.6 MB.
- They use mmap everywhere (no madvise); our default is also mmap (DEFAULT_READ_IO=false in util.rs, pread selectable with SHOP_IO=read).
- They overlap phases with `rayon::join` (links with parse, dedup with clicks); ours already overlaps links with parse.
- Exact-integer ID packing (pext) avoids hashing strings; our dense-id table does one hash lookup per ID instead.

## Ideas ranked by likely payoff for us
1. **Compress the Parquet output (snappy, or at minimum dictionary/RLE on repeated user_id).** Output volume is 4.7x smaller. On a scorer whose
   `/output` is a slow volume (overlay, network disk, tiny page cache) writing 75 MB can dominate a 0.3-1 s run, which would explain a large
   scorer-vs-local gap without any difference in compute. Test: time `docker_run.sh` with `/output` on a slower fs (e.g. a loop-mounted ext4 with `sync`
   or a bind mount to a throttled device), and with a snappy writer on the page bodies (snappy is ~500 MB/s per core; the writer is already
   parallel per partition). Cost: a few lines (a block-level snappy encoder, ~100 lines, or a literal-only fallback that still rewrites nothing).
   Keep an eye on the scorer's compare: snappy is a standard Parquet codec, so any reader accepts it.
2. **Check mmap vs pread on a cold file cache and on a VM.** Their mmap approach with 4 MB chunks was equal to ours warm. Our notes say pread was
   +0.5 s CPU on bare metal, but other teams report mmap slower on the scorer VM. Cannot be settled locally; consider one scorer A/B submission.
3. Exact-ID packing instead of a hash lookup for IDs <= 9 bytes: could replace the per-line ID-table probe for unlinked users. We measured link lookup at
   ~10 ns/event; at 20M events that is ~0.2 s CPU spread over 8 threads (~25 ms wall). Low payoff unless the ID table is the cache-miss hot spot.
4. Nothing else in their pipeline is a speedup over ours; do not trade our dense-id counting-sort design for their hash/sort design.

## Open question for the owner
The most plausible cause of 0.31 s is a scorer-side effect (disk or CPU) that this repo exposes only through the output size. Cheapest proof:
submit once with snappy Parquet and compare the scorer's time.
