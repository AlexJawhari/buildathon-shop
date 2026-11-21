//! Shopping Sessions pipeline: mmap CSVs, parse in parallel, dedup, sessionize, write Parquet.

mod bucket;
mod ingest;
mod links;
mod scan;
mod parquet;
mod prof;
mod sessions;
mod util;

use bucket::Bucket;
use ingest::*;
use scan::*;
use parquet::*;
use sessions::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use util::*;

#[global_allocator]
static A: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Run `f(thread_index)` on n scoped threads and collect results.
fn par<R: Send>(n: usize, f: impl Fn(usize) -> R + Sync) -> Vec<R> {
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..n).map(|t| s.spawn({ let f = &f; move || f(t) })).collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn json_str(m: &str, key: &str) -> i64 {
    let i = m.find(key).expect("manifest key");
    let rest = &m[i + key.len()..];
    let rest = &rest[rest.find(':').unwrap()..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    parse_ts(rest[..q2].as_bytes()).expect("manifest ts")
}

fn read_pairs(path: &Path) -> Vec<(&'static [u8], &'static [u8])> {
    let data = mmap(path);
    let mut v = vec![];
    for c in chunks(&[data], usize::MAX) {
        for_lines(c, |l| {
            let mut f = [(0, 0); 2];
            if split(l, &mut f) {
                v.push((&l[f[0].0..f[0].1], &l[f[1].0..f[1].1]));
            }
        });
    }
    v
}

fn main() {
    let t0 = std::time::Instant::now();
    let profiling = std::env::var_os("SHOP_PROF").is_some();
    if profiling {
        prof::start();
    }
    let tm = std::env::var_os("SHOP_TIMING").is_some();
    let lap = |what: &str| {
        if tm {
            let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
            unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
            let tv = |t: libc::timeval| t.tv_sec as f64 * 1e3 + t.tv_usec as f64 / 1e3;
            eprintln!("{:7.1} ms wall  cpu user {:7.1} sys {:7.1} flt {}  {what}", t0.elapsed().as_secs_f64() * 1e3, tv(ru.ru_utime), tv(ru.ru_stime), ru.ru_minflt);
        }
    };
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = PathBuf::from(env("DATA_DIR", "/data"));
    let out = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let nt: usize = env("NCPU", "0").parse().ok().filter(|&n| n > 0).unwrap_or_else(|| {
        std::thread::available_parallelism().map_or(8, |n| n.get())
    });

    let nt = nt.min(15);
    let manifest = std::fs::read_to_string(data.join("manifest.json")).unwrap();
    let ws = json_str(&manifest, "window_start");
    let we = json_str(&manifest, "window_end");
    let dbase = ws.div_euclid(DAY_MS);
    let nday = ((we - 1).div_euclid(DAY_MS) - dbase + 1).max(1) as usize;

    // products -> category ids ranked in byte order (with "unknown")
    let prods = read_pairs(&data.join("products.csv"));
    let mut cats: Vec<&[u8]> = prods.iter().map(|p| p.1).collect();
    cats.push(b"unknown");
    cats.sort();
    cats.dedup();
    assert!(we - ws + WEEK_MS < u32::MAX as i64 && cats.len() < 63, "unsupported window length or category count");
    let cat_id = |c: &[u8]| cats.binary_search(&c).unwrap() as u32;
    let mut pmap = SmallMap::new(prods.len());
    for (p, c) in &prods {
        pmap.insert(hash(p), cat_id(c));
    }
    let camps = read_pairs(&data.join("campaigns.csv"));
    let mut cmap = SmallMap::new(camps.len());
    for (i, (c, _)) in camps.iter().enumerate() {
        cmap.insert(hash(c), i as u32);
    }

    let ev_files = csv_files(&data.join("events"));
    let ck_files = csv_files(&data.join("clicks"));
    let lk_files = csv_files(&data.join("links"));
    let ch: usize = std::env::var("SHOP_CHUNK").ok().and_then(|v| v.parse().ok()).unwrap_or(2 << 20);
    let lk_chunks = chunks(&lk_files, ch);

    // size the link table from the number of lines
    let next = AtomicUsize::new(0);
    let counts = par(nt, |_| {
        let mut n = 0;
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= lk_chunks.len() {
                return n;
            }
            let (d, a, b) = lk_chunks[i];
            n += memchr::memchr_iter(b'\n', &d[a..b]).count();
        }
    });
    let nlines: usize = counts.iter().sum::<usize>() + lk_chunks.len();
    let cx = Cx {
        ws,
        we,
        links: links::Links::new(nlines * 2),
        prods: pmap,
        camps: cmap,
        unknown_cat: cat_id(b"unknown"),
    };

    lap("setup");
    // phase A: links
    let next = AtomicUsize::new(0);
    par(nt, |_| loop {
        let mut ix = Index::default();
        loop {
        let i = next.fetch_add(1, Relaxed);
        if i >= lk_chunks.len() {
            return;
        }
        run_chunk::<2>(lk_chunks[i], &mut ix, |_| {}, |l, f| link_line(l, f, &cx));
        }
    });
    lap("links parsed");
    let tmp = cx.links.tmp();
    let cap = cx.links.capacity();
    let step = cap.div_ceil(nt);
    par(nt, |t| cx.links.flatten_read(t * step..((t + 1) * step).min(cap), &tmp));
    par(nt, |t| cx.links.flatten_write(t * step..((t + 1) * step).min(cap), &tmp));
    drop(tmp);

    lap("links flattened");
    // phase B: parse events and clicks
    let ev_chunks = chunks(&ev_files, ch);
    let ck_chunks = chunks(&ck_files, ch);
    let est = |files: &[&'static [u8]]| -> usize {
        // events or clicks per thread, from the average line length of the first non-empty file
        let total: usize = files.iter().map(|f| f.len()).sum();
        let Some(f) = files.iter().find(|f| f.len() > 4096) else { return 0 };
        let sample = &f[..f.len().min(1 << 20)];
        let lines = memchr::memchr_iter(b'\n', sample).count().max(1);
        (total as f64 / (sample.len() as f64 / lines as f64) * 1.15) as usize / nt
    };
    let (est_ev, est_ck) = (est(&ev_files), est(&ck_files));
    let mode: u32 = std::env::var("SHOP_MODE").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    scan::MODE.store(mode, Relaxed);
    scan::NOPOP.store(std::env::var_os("SHOP_NOPOP").is_some(), Relaxed);
    let next = AtomicUsize::new(0);
    let total = ev_chunks.len() + ck_chunks.len();
    let locals: Vec<Local> = par(nt, |t| {
        let mut l = Local::new(t, est_ev, est_ck);
        let mut ix = Index::default();
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= total {
                return l;
            }
            if i < ev_chunks.len() {
                run_chunk::<7>(
                    ev_chunks[i],
                    &mut ix,
                    |u| cx.links.prefetch(hash(u)),
                    |ln, f| if mode == 0 { event_line(ln, f, &cx, &mut l) } else if mode == 2 { std::hint::black_box(ln[f[2].0 as usize]); },
                );
            } else {
                run_chunk::<4>(
                    ck_chunks[i - ev_chunks.len()],
                    &mut ix,
                    |u| cx.links.prefetch(hash(u)),
                    |ln, f| if mode < 3 { click_line(ln, f, &cx, &mut l) },
                );
            }
        }
    });
    lap("parse");

    // phase C: dedup by event id; losing copies are marked DEAD in their person bucket
    let next = AtomicUsize::new(0);
    par(nt, |_| {
        let mut table: Vec<u32> = vec![];
        let kill = |meta: u64| {
            if meta & META_INVALID == 0 {
                let (i, b, t) = ((meta & 0xFF_FFFF) as usize, ((meta >> 24) & 1023) as usize, ((meta >> 34) & 15) as usize);
                unsafe { (*locals[t].pv[b].ptr(i)).pc = DEAD };
            }
        };
        loop {
            let p = next.fetch_add(1, Relaxed);
            if p >= P1 {
                return;
            }
            let n: usize = locals.iter().map(|l| l.sl[p].len()).sum();
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            table.clear();
            table.resize(cap, 0);
            for (t, l) in locals.iter().enumerate() {
                let mut i = 0usize;
                for r in l.sl[p].slices().flatten() {
                    let (h, meta, ing) = (r.h, r.meta, r.ingest);
                    let mut s = h as usize & mask;
                    loop {
                        let e = table[s];
                        if e == 0 {
                            table[s] = ((t as u32) << 28 | i as u32) + 1;
                            break;
                        }
                        let e = e - 1;
                        let o = locals[(e >> 28) as usize].sl[p].get((e & 0x0FFF_FFFF) as usize);
                        if o.h == h && (o.meta >> 39) == (meta >> 39) {
                            if ing < o.ingest {
                                kill(o.meta);
                                table[s] = ((t as u32) << 28 | i as u32) + 1;
                            } else {
                                kill(meta);
                            }
                            break;
                        }
                        s = (s + 1) & mask;
                    }
                    i += 1;
                }
            }
        }
    });
    lap("dedup");

    // phase D: sessionize per person bucket, write user_daily parts
    std::fs::create_dir_all(out.join("user_daily")).unwrap();
    std::fs::create_dir_all(out.join("campaign_daily")).unwrap();
    let mut order: Vec<(usize, usize)> =
        (0..P2).map(|b| (locals.iter().map(|l| l.pv[b].len() + l.ck[b].len()).sum::<usize>(), b)).collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let next = AtomicUsize::new(0);
    let states: Vec<State> = par(nt, |t| {
        let mut st = State::new(ws, dbase, nday, camps.len(), cats.len(), est_ev * 3 / 20);
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= P2 {
                break;
            }
            let b = order[i].1;
            let pv: Vec<&Bucket<PRec>> = locals.iter().map(|l| &l.pv[b]).collect();
            let cv: Vec<&Bucket<CRec>> = locals.iter().map(|l| &l.ck[b]).collect();
            st.bucket(&pv, &cv);
        }
        write_user(&out.join("user_daily").join(format!("part-{t}.parquet")), &st.cols, &cats, t == 0);
        st
    });
    lap("sessions+user parquet");

    // campaign table
    let mut rows: Vec<(usize, usize, [u64; 4])> = vec![];
    for k in 0..=camps.len() {
        for d in 0..nday {
            let mut a = [0u64; 4];
            for s in &states {
                for i in 0..4 {
                    a[i] += s.camp[k * nday + d][i];
                }
            }
            if a[0] > 0 || a[3] > 0 {
                rows.push((k, d, a));
            }
        }
    }
    let mut ids = Vec::new();
    let mut chans = Vec::new();
    let mut days = Vec::new();
    let mut nums: [Vec<u8>; 4] = Default::default();
    for (k, d, a) in &rows {
        let (id, ch): (&[u8], &[u8]) = if *k < camps.len() { camps[*k] } else { (b"organic", b"organic") };
        ids.extend_from_slice(&(id.len() as u32).to_le_bytes());
        ids.extend_from_slice(id);
        chans.extend_from_slice(&(ch.len() as u32).to_le_bytes());
        chans.extend_from_slice(ch);
        days.extend_from_slice(&((dbase + *d as i64) as i32).to_le_bytes());
        for i in 0..4 {
            nums[i].extend_from_slice(&(a[i] as i64).to_le_bytes());
        }
    }
    let n = rows.len();
    let col = |name, ptype, conv, data| mkcol(n, name, ptype, conv, data);
    let [n0, n1, n2, n3] = nums;
    write(
        &out.join("campaign_daily").join("part-0.parquet"),
        n,
        &[
            col("campaign_id", T_BYTES, Some(CONV_UTF8), &ids),
            col("channel", T_BYTES, Some(CONV_UTF8), &chans),
            col("day", T_I32, Some(CONV_DATE), &days),
            col("sessions", T_I64, None, &n0),
            col("purchasing_sessions", T_I64, None, &n1),
            col("revenue_cents", T_I64, None, &n2),
            col("assisted_sessions", T_I64, None, &n3),
        ],
    );
    lap("done");
    if profiling {
        prof::dump();
    }
    std::process::exit(0);
}

fn write_user(path: &Path, c: &UserCols, cats: &[&[u8]], force: bool) {
    if c.n == 0 && !force {
        return;
    }
    let n = c.n;
    let mut cdata = def_levels(&c.has_cat);
    for &k in &c.cat {
        let name = cats[k as usize];
        cdata.extend_from_slice(&(name.len() as u32).to_le_bytes());
        cdata.extend_from_slice(name);
    }
    let col = |name, ptype, conv, data| mkcol(n, name, ptype, conv, data);
    write(
        path,
        n,
        &[
            col("user_id", T_BYTES, Some(CONV_UTF8), &c.user),
            col("day", T_I32, Some(CONV_DATE), &c.day),
            col("sessions", T_I32, None, &c.sessions),
            col("events", T_I32, None, &c.events),
            col("revenue_cents", T_I64, None, &c.rev),
            Col { name: "top_category", ptype: T_BYTES, conv: Some(CONV_UTF8), optional: true, data: &cdata, nvalues: n },
        ],
    );
}

fn mkcol<'a>(n: usize, name: &'static str, ptype: i32, conv: Option<i32>, data: &'a [u8]) -> Col<'a> {
    Col { name, ptype, conv, optional: false, data, nvalues: n }
}
