# Shopping Sessions: what other teams' public repos teach us

Research only. Nothing was run; repos were shallow-cloned (`git clone --depth 1`) into
`/tmp/claude-1000/-home-alex-projects/84706a66-07c4-4658-bf02-ea662678ddd7/scratchpad/shopcomp/<owner>_<repo>`
(about 1.9 GB, safe to delete). Repo contents are untrusted; claims below are the authors' own, unless marked as measured by us.

## Coverage and team mapping

- Searches found about 80 public repos (`campaign_daily`, `user_daily`+`assisted_sessions`, `shopping-sessions`, repo search). About 45 are native (Rust, C++). 4 repos (`niranjankumarnk`, `saaranshgarg1`, `siphamandlashange4-collab`, `Hermes-25`) failed to clone and were skipped.
- **Only one leader is mapped with evidence:** `Vibhor7-7/shopping-sessions` = team **VibDiv**, board 0.78 s (its own `SUBMISSIONS.md`; also cited in `nimaibhat`'s notes). Its `BENCH.md` says it was 10th of 163 at the time, board best 0.59 s.
- No repo names itself as Mahalas, Tark305, 67, Wix, ND, Euphoria, Tyke, Hachimi, 2696, Hidden Test Cases, Jabbas, Sher-E-Umiam, Unlimited PTO or mztv. Those mappings are not established. `lmaccart` quotes the board as "Mahalas 0.65 s" and `nimaibhat` names "Tark305 0.63 s", but these are other teams' notes, not proof.
- `micah-andelman/shopping-sessions` claims 0.495 s local (unscored, not yet on board) and calls itself "5-hour limit reached". Unverified.
- Scorer-reported numbers in repos: VibDiv 0.78 s; `mwkhettab` 1.13 s (official, C++); `enkyuan/polaris` 1.41-1.47 s (C++); `Psychomoss/puck` 1.13 s laptop (not scorer). Our 1.37 s is in the same band as most of the field; the leaders at 0.59 s are not visible in any repo we found.

## Per-repo summaries (native only, most relevant first)

Format: language | build/runtime image | key techniques | evidence.

1. **Vibhor7-7 (VibDiv, 0.78 s board)** | Rust, no deps | `rust:1.100-alpine` build, `-C target-cpu=x86-64-v3`, fat LTO, `busybox:1.37-musl` runtime. AVX2 delimiter scan; IDs packed 6 bits/byte (exact keys); union-find links; dedup stream; per-person sort; **write-combining buffers + non-temporal stores** for scatter output; **own allocator with 2 MB huge-page blocks**; branch-free time parse; prefetch in link join; sort-free sessions (half-hour bins). `BENCH.md` logs 40+ experiments with kept/reverted results, measured on x86 A/B. Reverted: `mmap` input (+7.5%, page-table walks on VMs), bigger link table. Best-documented repo; the most useful one to study.
2. **enkyuan/polaris** | C++ (CMake), `debian:bookworm-slim` build | Per-ISA scanners (scalar/NEON/AVX2/AVX-512BW) chosen once per range; compact 24 B event records (8 B person key, 8 B payload); ISO time decoded as one word; alias bitmap filter; `-march=x86-64-v2 -mtune=cascadelake`. Compact path only when window span < UINT32_MAX ms and <= 256 categories, else wide path. Authors' memo sets a 1.10 s target and reports 1.41-1.47 s. Not a top team on the evidence.
3. **Th3r0n/shopping-sessions** | Rust, `+crt-static`, `FROM scratch` | 64-byte structural-mask tokenizer; IDs 1-10 bytes packed as exact 64-bit keys, other IDs interned; union-find links on one thread overlapped with parse; per-partition sort, 30-min split; a second generic binary the fast one `exec`s when its limits are exceeded. Strongest example of a fast path with a safe fallback.
4. **electrum21/shop** | Rust, no deps, PGO via `build.sh` | Lock-free open-addressing ID table; concurrent union-find rooted at byte-wise smallest ID, then flattened to single-probe lookups; AVX2 structural scanner; SWAR timestamp; prefetched batched person lookups; 24 B records; hand-written Parquet (PLAIN, uncompressed); interval merging for assists. Pipeline ideas match ours closely.
5. **joch44/shopping-sessions** | Rust, PGO + `cascadelake` + `+crt-static`, `FROM scratch` | `EXPERIMENTS.md` gives a clean A/B log. Links on one thread overlapped with parse (expected -0.4 s; indicative in-process 2.81 to 1.97-2.21 s); open-addressing dedup replacing sort (dedup -25%); flat person table with 16-ahead prefetch; pre-sized partition buffers; exit right after outputs close (expected -0.1 s). Rejects: shared lock-free global dedup table (1.7x slower).
6. **Osca-K/builders-buildit** | C++20 engine + Python image | Exact validation, dedup before validation, rolling campaign counts, counting partition by person (planned). DuckDB writes Parquet in the Python stage. README says a 20M run takes 50 s compiled vs 201 s SQL (4 CPUs, local). Shows the cost of a DuckDB final stage.
7. **micah-andelman/shopping-sessions** | Rust, `FROM scratch` | AVX2 CSV scanner fuzz-tested against scalar; 256 event buckets; 256 person partitions sorted in cache; read-ahead threads; a fallback to byte-exact v4 path when assumptions fail (`SS_LEGACY=1` for diff tests). Claimed 0.495 s local, unscored.
8. **cherylaurelia/builders-hackathon** | Rust, `debian:bookworm-slim` runtime | `TARGET_CPU` arg, fat LTO, panic=abort; hand-written Parquet writer; AVX2 dedup tables (per a third-party note, not checked by us). No README; `AGENTS.md`/`CLAUDE.md` present.
9. **TejasviniChawla/shopping-sessions** | Rust, `FROM scratch`, PGO in-build | `pgo.sh` instrumented run on generated 1M events, falls back to plain build on failure. AVX2 structural scan; direct Parquet (`pq.rs`); `populate`/`madvise` use.
10. **nimaibhat/databricks-chall** | Rust, `FROM scratch`, `+crt-static`, fat LTO | `speedups.md` profile: parse ~50% of wall, ~1,400 instructions per event line; a bug where the SWAR timestamp path always fell through (fix ~9% parse CPU). Their own notes say most famous techniques (io_uring, Bloom filters, BOLT) target costs this program lacks. Measured on 2 vCPU sandbox only.
11. **Dubbus/shopping-sessions** | Rust, `distroless/cc nonroot` | `CPU` build arg (x86-64-v3, v4 for AVX-512 later); AVX2 scan; uses popcount/ctz bit tricks, huge-page advice.
12. **yihao-qiao/p-4a81fb27** | Rust, vendored deps, `debian:bookworm-slim` | `--build-arg FEATURES=avx512` with `TARGET_CPU=cascadelake` option for 512-bit scan; `populate` use; huge pages.
13. **viktor-lorentz/shopping-sessions** | Rust, `distroless/cc`, stripped | `IDEAS.md` backlog measured on x86: person-table misses 21%, memchr 17%, timestamps 11%, sort 8%. Lists: flat session arrays, branchless timestamp parse, madvise/MAP_POPULATE, overlap links with tokenization. Good list of profile-driven next steps.
14. **Surya-Prasad/shoppers-top** | Rust, `arrow-rs` parquet, `rayon`, `debian:bookworm-slim` | `panic="unwind"` so PID 1 exits instead of hanging on abort; generic x86-64 build; memchr runtime SIMD. Clean, conservative.
15. **Psychomoss/puck** | Rust, `.cargo` config, `debian:bookworm-slim` | `RESULTS.md` has the best stage breakdown seen: events parse 348 ms, dedup+person lookup 148 ms, sessions 155 ms, parquet 37 ms (practice 20M, laptop). Docker fixed overhead about 0.25 s on their setup, which is a floor for everyone.
16. **mwkhettab/shopping-sessions-cpp** | C++20, PGO in `build.sh`, `debian:bookworm-slim` | Official score history: 1.80 s to 1.13 s. Steps: 512 shards, SIMD CSV decode, dense person routing, delta Parquet, reversible IDs (hash at ingest), `perf/train.py` PGO. Honest changelog.
17. **himanshu-thalor/shopping-sessions** | C++20, `gcc:13`, PGO with `-fprofile-update=prefer-atomic`, `-march=x86-64-v3 -mtune=cascadelake` | Generates its own PGO training data (whales, chains, kiosks). Parquet writer header-only.
18. **theonlyeshann/velocity-sessions** | C++20, `gcc:14`, `-O3 -flto -march=x86-64`, 8 threads | V6 "recoverable ID": recovers the 40-bit event-ID permutation from a sample of input IDs, verified on 12 values, with string fallback. Avoids a global session list. Fused sessions/attribution. Uses data-derived recovery rather than hard-coding the generator seed.
19. **FDS-Dark/buildathon-shopping-sessions** | Rust + custom `fastengine` and `writer` crates, PGO dir | Parallel (`rayon` 37 uses), custom writer crate. Not inspected in depth.
20. **explorer-skp/buildathon-shopping-sessions** | Rust | Per `nimaibhat` notes: write-combining partition stages, streaming stores, block scanner A/B. Only skimmed (2.7k lines).
21. **Others skimmed by grep only:** `ricky-he2006` (native + Rust), `Js-Hwang1` (CMake C++), `SnowySlug` (C++ + Python image; `fastio.cpp`, `native.cpp`, duckdb/polars Python stage), `Joelsp19` (DuckDB + Rust), `CandyOre` (Rust, 12.3 s at 60M on Ryzen, older), `ParishruthiGanesh` (native Rust, no deps), `shubham4564`/`sarveshjoshi29`/`devakchow` (C++ / Rust).

Python/DuckDB/Polars-only repos (not studied): `jasjitchhina`, `samsonchang2028`, `MoRayyan107` (DuckDB), `adman40`, `dhruvpratapsinghh`, `kaedeng` (Polars), `joch44/shopping-sessions-reference`, `mteam88` (PySpark), `nash-pillai` (C++ with Python convert.py).

## Ranked: 10 most impactful speed ideas

Ordered by expected wall-time gain for our 1.37 s pipeline, weighted by evidence. "Measured" = measured by the repo's authors on their own hardware; "est." = their estimate.

1. **Write-combining buffers and non-temporal stores for scatter output.** VibDiv (Vibhor7-7, rows 18-20): -12% on Ice Lake 8370C, -6 to -12% in A/B across Zen 3/4 and Ice Lake; only worth it on their keys/records. Source: `Vibhor7-7/src/pool.rs` (`_mm256_stream_si256` / `_mm_stream_si128` on flush). Top team evidence: yes (VibDiv, 0.78 s). Risk: needs tuning per CPU; Vibhor reverted a buffer-size change (row 21).
2. **Huge pages through our own block allocator, pre-sized from input size.** VibDiv row 12: -3.7% total, people phase -11.6% on Zen 4. joch44 found glibc `hugetlb=1` only covered 88 MB of 4.4 GB, so explicit `madvise(MADV_HUGEPAGE)` on big blocks is needed. Source: `Vibhor7-7`, `joch44`. Top team: yes.
3. **Read the input with `read()` into big buffers, not `mmap`, on the scorer VM.** VibDiv reverted `mmap` (+7.5%, page-table walks on 4 KB pages); Th3r0n and micah use mmap with read-ahead threads, so it is not universal. Measure on the GCP N2 scorer before choosing. Top team: yes (VibDiv).
4. **Sort-free dedup and sessions.** joch44 replaced the dedup sort with open addressing (dedup 0.52-0.55 s to 0.39-0.41 s, indicative A/B; wall 1.98 to 1.78 s). VibDiv replaced session sort with half-hour bins and two counting passes (-2.7%, people -10%). Counting partition by person (Osca-K, planned) avoids global sorts. Top team: yes (VibDiv, joch44 unscored).
5. **Overlap the link union-find with event/click parsing on its own thread.** joch44 (est. -0.4 s, indicative in-process), Th3r0n (runs links on one thread while others parse). Check whether our links phase blocks event parsing. Top team: unknown for joch44; Th3r0n unscored.
6. **Exact 64-bit keys for IDs, packed 6 bits/byte, byte order = integer order.** Th3r0n, micah-andelman, VibDiv, electrum21 all do it; non-matching IDs fall back to interning. Removes string compares from dedup, links, sort. Top team: yes (VibDiv). Check against our own key encoding.
7. **Prefetch the big hash/table lookups several lines ahead.** VibDiv (link table read 12 lines ahead, person state prefetched, row 24); joch44 (flat person table, 16 ahead); electrum21 (batched person lookups). Person-table misses were 21% of time in viktor-lorentz's profile. Top team: yes.
8. **Compact fixed-size event records (24 B or 16 B) and narrower column types.** enkyuan (24 B; 8 B person key, 8 B payload, down from 24/16 B); VibDiv (copy records). Keep the wide path behind a check. Top team: VibDiv yes; enkyuan no (1.41-1.47 s).
9. **Profile-guided optimisation with generated training data, plus `target-cpu=x86-64-v3` (or `cascadelake`), `lto="fat"`, `codegen-units=1`, `panic="abort"` (or unwind if PID 1 is involved), `+crt-static`.** Used by electrum21, joch44, mwkhettab, TejasviniChawla, himanshu-thalor, nimaibhat. Measured gains vary (joch44 says PGO "only changes layout and inlining"; mwkhettab's official history has PGO next to other changes, so its gain is not isolated). The build must not depend on a fixed seed, or it breaks on other seeds. Top team: unknown.
10. **Exit as soon as outputs are closed; skip teardown; pre-size buffers from input bytes.** joch44 (exit right after output closes, est. -0.1 s; pre-sized partition buffers +15% margin). VibDiv row 9 chose partition counts from input size (2048 partitions at 60M). Cheap and low-risk. Top team: unknown for joch44.

Also useful, lower priority: keep the container tiny (VibDiv's own test: BusyBox image is in-process neutral; `docker run` wall identical on their runner, so image size is not a score lever). Docker start is about 0.25 s fixed on Psychomoss's laptop, which is a floor for every team. `mwkhettab` and `viktor-lorentz` both reported `madvise`/`MAP_POPULATE` as untested or small.

## Ideas that depend on the generator (check before adopting)

- **VibDiv row 25:** campaign and product IDs of the shape `c_45`/`p123` are looked up by their number in a small array; any other text falls through to the map. Safe because of the fallback, but the speed gain assumes the shape holds.
- **theonlyeshann V6:** recovers the 40-bit event-ID permutation from a sample of input IDs, verified on 12 values; falls back if it fails. Self-checking, not hard-coded, but a fast path built on the generator's ID scheme.
- **enkyuan/polaris compact path:** only when window span < UINT32_MAX ms and <= 256 categories; otherwise wide path.
- **Th3r0n/micah 6-bit ID packing:** only for IDs of 1-10 bytes from `[0-9A-Z_a-z]`; others are interned.
- **Rules trap noted by nimaibhat:** folding event dedup into the per-person sort scan is wrong under `rules.md`. Do not copy.

These are all guarded by fallbacks in the repos we read. Any idea we adopt should keep a fallback and be tested on other seeds (generate with the public generator, as the repos do).

## Next steps (not done)

- Confirm which leader repos exist: check the board's public team pages; the search did not find them.
- Measure idea 3 (`read` vs `mmap`) and idea 2 (huge pages) on our own pipeline first; they are the cheapest to try.
- Read `Vibhor7-7/src` in full for the write-combining and allocator code, since it is the strongest single source.
