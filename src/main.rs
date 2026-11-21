//! Shopping Sessions pipeline: mmap CSVs, parse in parallel, dedup, sessionize, write Parquet.

mod ingest;
mod links;
mod parquet;
mod sessions;
mod util;

use ingest::*;
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
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = PathBuf::from(env("DATA_DIR", "/data"));
    let out = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let nt: usize = env("NCPU", "0").parse().ok().filter(|&n| n > 0).unwrap_or_else(|| {
        std::thread::available_parallelism().map_or(8, |n| n.get())
    });

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
    const CH: usize = 2 << 20;
    let lk_chunks = chunks(&lk_files, CH);

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
        unknown_cat: cat_id(b"unknown") as u16,
    };

    // phase A: links
    let next = AtomicUsize::new(0);
    par(nt, |_| loop {
        let i = next.fetch_add(1, Relaxed);
        if i >= lk_chunks.len() {
            return;
        }
        for_lines(lk_chunks[i], |l| link_line(l, &cx));
    });
    let tmp = cx.links.tmp();
    let cap = cx.links.capacity();
    let step = cap.div_ceil(nt);
    par(nt, |t| cx.links.flatten_read(t * step..((t + 1) * step).min(cap), &tmp));
    par(nt, |t| cx.links.flatten_write(t * step..((t + 1) * step).min(cap), &tmp));
    drop(tmp);

    // phase B: parse events and clicks
    let ev_chunks = chunks(&ev_files, CH);
    let ck_chunks = chunks(&ck_files, CH);
    let next = AtomicUsize::new(0);
    let total = ev_chunks.len() + ck_chunks.len();
    let locals: Vec<Local> = par(nt, |_| {
        let mut l = Local::new();
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= total {
                return l;
            }
            if i < ev_chunks.len() {
                for_lines(ev_chunks[i], |ln| event_line(ln, &cx, &mut l));
            } else {
                for_lines(ck_chunks[i - ev_chunks.len()], |ln| click_line(ln, &cx, &mut l));
            }
        }
    });

    // phase C: dedup by event id, route survivors to person buckets
    let next = AtomicUsize::new(0);
    let pouts: Vec<Vec<Vec<PRec>>> = par(nt, |_| {
        let mut outb: Vec<Vec<PRec>> = (0..P2).map(|_| Vec::new()).collect();
        let mut table: Vec<u64> = vec![];
        loop {
            let p = next.fetch_add(1, Relaxed);
            if p >= P1 {
                return outb;
            }
            let n: usize = locals.iter().map(|l| l.ev[p].len()).sum();
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            table.clear();
            table.resize(cap, 0);
            let rec = |e: u64| &locals[(e >> 32) as usize].ev[p][(e & 0xFFFF_FFFF) as usize];
            for (t, l) in locals.iter().enumerate() {
                for (i, r) in l.ev[p].iter().enumerate() {
                    let mut s = r.h as usize & mask;
                    loop {
                        let e = table[s];
                        if e == 0 {
                            table[s] = ((t as u64) << 32 | i as u64) + 1;
                            break;
                        }
                        let o = rec(e - 1);
                        if o.h == r.h && o.chk == r.chk {
                            if r.ingest < o.ingest {
                                table[s] = ((t as u64) << 32 | i as u64) + 1;
                            }
                            break;
                        }
                        s = (s + 1) & mask;
                    }
                }
            }
            for &e in &table {
                if e != 0 {
                    let r = rec(e - 1);
                    if r.ty != INVALID {
                        outb[bucket(r.pkey)].push(PRec { pkey: r.pkey, ts: r.ts, uptr: r.uptr, price: r.price, cat: r.cat, ty: r.ty });
                    }
                }
            }
        }
    });

    // phase D: sessionize per person bucket, write user_daily parts
    std::fs::create_dir_all(out.join("user_daily")).unwrap();
    std::fs::create_dir_all(out.join("campaign_daily")).unwrap();
    let next = AtomicUsize::new(0);
    let states: Vec<State> = par(nt, |t| {
        let mut st = State::new(dbase, nday, camps.len(), cats.len());
        loop {
            let b = next.fetch_add(1, Relaxed);
            if b >= P2 {
                break;
            }
            let mut evs: Vec<PRec> = pouts.iter().flat_map(|o| o[b].iter().copied()).collect();
            let mut cks: Vec<CRec> = locals.iter().flat_map(|l| l.ck[b].iter().copied()).collect();
            st.bucket(&mut evs, &mut cks);
        }
        write_user(&out.join("user_daily").join(format!("part-{t}.parquet")), &st.rows, &cats, t == 0);
        st
    });

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
    let col = |name, ptype, conv, data: Vec<u8>| Col { name, ptype, conv, optional: false, data, nvalues: n };
    let [n0, n1, n2, n3] = nums;
    write(
        &out.join("campaign_daily").join("part-0.parquet"),
        n,
        &[
            col("campaign_id", T_BYTES, Some(CONV_UTF8), ids),
            col("channel", T_BYTES, Some(CONV_UTF8), chans),
            col("day", T_I32, Some(CONV_DATE), days),
            col("sessions", T_I64, None, n0),
            col("purchasing_sessions", T_I64, None, n1),
            col("revenue_cents", T_I64, None, n2),
            col("assisted_sessions", T_I64, None, n3),
        ],
    );
    std::process::exit(0);
}

fn write_user(path: &Path, rows: &[UserRow], cats: &[&[u8]], force: bool) {
    if rows.is_empty() && !force {
        return;
    }
    let n = rows.len();
    let (mut u, mut d, mut s, mut e, mut r) = (vec![], vec![], vec![], vec![], vec![]);
    let mut valid = Vec::with_capacity(n);
    let mut cv = vec![];
    for x in rows {
        let name = unpack(x.name);
        u.extend_from_slice(&(name.len() as u32).to_le_bytes());
        u.extend_from_slice(name);
        d.extend_from_slice(&x.day.to_le_bytes());
        s.extend_from_slice(&x.sessions.to_le_bytes());
        e.extend_from_slice(&x.events.to_le_bytes());
        r.extend_from_slice(&(x.rev as i64).to_le_bytes());
        if x.cat != NO_CAT {
            let c = cats[x.cat as usize];
            cv.extend_from_slice(&(c.len() as u32).to_le_bytes());
            cv.extend_from_slice(c);
            valid.push(true);
        } else {
            valid.push(false);
        }
    }
    let mut cdata = def_levels(&valid);
    cdata.extend_from_slice(&cv);
    let col = |name, ptype, conv, data| Col { name, ptype, conv, optional: false, data, nvalues: n };
    write(
        path,
        n,
        &[
            col("user_id", T_BYTES, Some(CONV_UTF8), u),
            col("day", T_I32, Some(CONV_DATE), d),
            col("sessions", T_I32, None, s),
            col("events", T_I32, None, e),
            col("revenue_cents", T_I64, None, r),
            Col { name: "top_category", ptype: T_BYTES, conv: Some(CONV_UTF8), optional: true, data: cdata, nvalues: n },
        ],
    );
}
