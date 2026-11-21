# Shopping Sessions: notes

Native Rust pipeline, static glibc binary in a `FROM scratch` image (~8 MB). No Python, no DuckDB.

## Pipeline

1. `links/`: SIMD line index, lock-free hash table of every ID in a valid link line, lock-free union-find whose
   root is always the byte-wise smallest string (the person's name). Table is flattened so a lookup gives the name directly.
2. `events/`, `clicks/`: mmap, 2 MB chunks, 8 threads. Per chunk an AVX2 index of commas/newlines/quote masks,
   then the CSV dialect check from the index alone (quotes only around a whole field: quote count of the line must be
   2 x number of fields that start with a quote, and each such field must end with a quote). Hand-rolled ts parse
   (F1/F2 SWAR + date cache, F3 digits). Each valid event goes to its person bucket (`PRec`, 24 B) and a slim dedup
   record (`Slim`, 20 B) goes to an event-id partition. Clicks go straight to person buckets (no click dedup needed:
   copies are identical, and credit/assist are idempotent under duplicates).
3. Dedup: per event-id partition, a small hash table; the loser copy is marked DEAD in its person bucket (random write).
   An invalid winner kills valid later copies (dedup before validation).
4. Per person bucket: counting sort by person, sort by ts, sessionize, credit clicks (two pointers), assists
   (per click, sessions in [t, t+7d] not yet covered by that campaign, added per day via day boundaries), aggregate.
5. Output: hand-written Parquet (PLAIN, uncompressed, one row group per file), one user_daily file per thread.

Person key = hash of the name for unlinked IDs, packed (ptr,len) of the root string for linked ones.
Limits: window < ~42 days (u32 ms offsets), < 63 categories, <= 15 threads (asserted).

## Checks

- `python3 scripts/run_fixtures.py fixtures -- target/release/shop`: 36/36.
- `scripts/reference.py`: slow independent implementation straight from rules.md (matches fixtures and dev-answer exactly).
- `scripts/crosscheck.sh SEED EVENTS CLICKS`: generate, run reference and binary, diff.
- `SHOP_CHUNK=<bytes>` shrinks chunks to stress chunk boundaries; `SHOP_TIMING=1` prints phase wall/CPU;
  `SHOP_PROF=1` + `scripts/profsum.sh` is a sampling profiler (perf is not permitted on this box).

## Experiments (hypothesis -> number -> keep/drop). Box is shared and loaded (load 10-16 on 12 threads): wall times
are noisy, so user+sys CPU is the main number. 20M events / 5M clicks, 143 event files.

| Experiment | Result | Verdict |
| --- | --- | --- |
| Baseline: byte-loop split, full 48 B record repartitioned by event id then person | 3.2 s wall, user 8.5 s, dedup phase 0.8 s | replaced |
| Slim dedup records + kill list into person buckets, counting-sort person grouping, 24 B records | dedup 0.03 s, sessions 0.2 s, user 6.7 s | keep |
| Scalar hash collided ("p10" vs "p1": length xor-ed into the data word) | dev set 1 row wrong | fixed (length multiplied in), unit test |
| AVX2 index: 16 unconditional writes x 3 masks | build 0.4 ns/B | replaced |
| Index: comma index (8 writes) + newline index (2 writes) + per-block quote mask with prefix counts | build 0.17 ns/B | keep |
| Ring of parsed lines for prefetch vs prefetch from index arithmetic (line k+12 assumed well formed) | simpler, same speed | keep arithmetic |
| ts: SWAR time-of-day + date cache | ts parse about 2x cheaper; fuzz-tested against slow parser (2M cases) | keep |
| MADV_POPULATE_READ on input chunks | no measurable change | kept (harmless) |
| Per-thread arena (THP madvise + MADV_POPULATE_WRITE) for bucket blocks instead of Vec growth | fewer faults when THP is available; faults vary 120k-400k run to run with memory pressure | keep |
| Streaming parquet columns, no row structs, no assembled file buffer | removes ~200 MB of temp memory at 20M | keep |
| Docker (scorer flags, 8 cpus): 20M events | about 1.6 s wall under load, 8 MB image | |
