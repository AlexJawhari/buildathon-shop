//! Per-person sessionization, click credit/assists and aggregation.

use crate::bucket::Bucket;
use crate::ingest::*;
use crate::util::*;

const GAP_MS: i64 = 30 * 60 * 1000;
pub const NO_CAT: u16 = u16::MAX;

/// user_daily rows, stored column-wise in PLAIN Parquet encoding as they are produced.
#[derive(Default)]
pub struct UserCols {
    pub n: usize,
    pub user: Vec<u8>,
    pub day: Vec<u8>,
    pub sessions: Vec<u8>,
    pub events: Vec<u8>,
    pub rev: Vec<u8>,
    pub has_cat: Vec<bool>,
    pub cat: Vec<u16>,
}

struct Sess {
    start: i64,
    day: u32,
    rev: u64,
    purch: bool,
}

pub struct State {
    pub ws: i64,
    pub dbase: i64,
    pub nday: usize,
    pub organic: u32,
    pub camp: Vec<[u64; 4]>, // [(campaign, day)] -> sessions, purchasing, revenue, assisted
    pub cols: UserCols,
    sess: Vec<Sess>,
    dfirst: Vec<usize>,
    covered: Vec<u32>,
    touched: Vec<u32>,
    cat_sum: Vec<u64>,
    cat_touched: Vec<u16>,
    // per-bucket scratch
    table: Vec<u32>,
    pkeys: Vec<u64>,
    cnt: Vec<u32>,
    recs: Vec<(u32, PRec)>,
    sorted: Vec<PRec>,
    crecs: Vec<(u32, CRec)>,
    csorted: Vec<CRec>,
}

#[inline(always)]
fn slot(pkey: u64, mask: usize) -> usize {
    ((pkey.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 20) as usize) & mask
}

impl State {
    pub fn new(ws: i64, dbase: i64, nday: usize, ncamp: usize, ncat: usize, est_rows: usize) -> State {
        State {
            ws,
            dbase,
            nday,
            organic: ncamp as u32,
            camp: vec![[0; 4]; (ncamp + 1) * nday],
            cols: UserCols {
                n: 0,
                user: Vec::with_capacity(est_rows * 14),
                day: Vec::with_capacity(est_rows * 4),
                sessions: Vec::with_capacity(est_rows * 4),
                events: Vec::with_capacity(est_rows * 4),
                rev: Vec::with_capacity(est_rows * 8),
                has_cat: Vec::with_capacity(est_rows),
                cat: Vec::with_capacity(est_rows),
            },
            sess: vec![],
            dfirst: vec![0; nday + 2],
            covered: vec![0; ncamp + 1],
            touched: vec![],
            cat_sum: vec![0; ncat],
            cat_touched: vec![],
            table: vec![],
            pkeys: vec![],
            cnt: vec![],
            recs: vec![],
            sorted: vec![],
            crecs: vec![],
            csorted: vec![],
        }
    }

    fn grow(&mut self) {
        let cap = (self.table.len() * 2).max(4096);
        self.table.clear();
        self.table.resize(cap, 0);
        for (id, &k) in self.pkeys.iter().enumerate() {
            let mut s = slot(k, cap - 1);
            while self.table[s] != 0 {
                s = (s + 1) & (cap - 1);
            }
            self.table[s] = id as u32 + 1;
        }
    }

    /// Group one person bucket's events and clicks per person and process each person.
    pub fn bucket(&mut self, pvs: &[&Bucket<PRec>], cks: &[&Bucket<CRec>]) {
        self.pkeys.clear();
        self.cnt.clear();
        self.recs.clear();
        self.table.clear();
        self.table.resize(4096, 0);
        for v in pvs {
            for r in v.slices().flatten() {
                if r.pc == DEAD {
                    continue;
                }
                if self.pkeys.len() * 2 >= self.table.len() {
                    self.grow();
                }
                let mask = self.table.len() - 1;
                let mut s = slot(r.pkey, mask);
                let id = loop {
                    let e = self.table[s];
                    if e == 0 {
                        let id = self.pkeys.len() as u32;
                        self.pkeys.push(r.pkey);
                        self.cnt.push(0);
                        self.table[s] = id + 1;
                        break id;
                    }
                    if self.pkeys[e as usize - 1] == r.pkey {
                        break e - 1;
                    }
                    s = (s + 1) & mask;
                };
                self.cnt[id as usize] += 1;
                self.recs.push((id, *r));
            }
        }
        let np = self.pkeys.len();
        if np == 0 {
            return;
        }
        // counting sort by person id
        let mut off = vec![0u32; np + 1];
        for i in 0..np {
            off[i + 1] = off[i] + self.cnt[i];
        }
        let mut pos = off.clone();
        self.sorted.clear();
        self.sorted.resize(self.recs.len(), PRec { pkey: 0, uptr: 0, ts: 0, pc: 0 });
        for &(id, r) in &self.recs {
            self.sorted[pos[id as usize] as usize] = r;
            pos[id as usize] += 1;
        }
        // clicks of persons that have events
        self.crecs.clear();
        let mut ccnt = vec![0u32; np + 1];
        let mask = self.table.len() - 1;
        for v in cks {
            for c in v.slices().flatten() {
                let mut s = slot(c.pkey, mask);
                loop {
                    let e = self.table[s];
                    if e == 0 {
                        break;
                    }
                    if self.pkeys[e as usize - 1] == c.pkey {
                        ccnt[e as usize - 1] += 1;
                        self.crecs.push((e - 1, *c));
                        break;
                    }
                    s = (s + 1) & mask;
                }
            }
        }
        let mut coff = vec![0u32; np + 1];
        for i in 0..np {
            coff[i + 1] = coff[i] + ccnt[i];
        }
        let mut cpos = coff.clone();
        self.csorted.clear();
        self.csorted.resize(self.crecs.len(), CRec { pkey: 0, cid: 0, ts: 0, camp: 0 });
        for &(id, c) in &self.crecs {
            self.csorted[cpos[id as usize] as usize] = c;
            cpos[id as usize] += 1;
        }
        let mut sorted = std::mem::take(&mut self.sorted);
        let mut csorted = std::mem::take(&mut self.csorted);
        for id in 0..np {
            let evs = &mut sorted[off[id] as usize..off[id + 1] as usize];
            let cks = &mut csorted[coff[id] as usize..coff[id + 1] as usize];
            evs.sort_unstable_by_key(|r| r.ts);
            cks.sort_unstable_by_key(|r| r.ts);
            self.person(evs, cks);
        }
        self.sorted = sorted;
        self.csorted = csorted;
    }

    fn flush_day(&mut self, name: u64, day: i32, sessions: u32, events: u32, rev: u64) {
        let mut cat = NO_CAT;
        if !self.cat_touched.is_empty() {
            let mut best = 0;
            for &k in &self.cat_touched {
                let s = self.cat_sum[k as usize];
                if cat == NO_CAT || s > best || (s == best && k < cat) {
                    cat = k;
                    best = s;
                }
            }
            for &k in &self.cat_touched {
                self.cat_sum[k as usize] = 0;
            }
            self.cat_touched.clear();
        }
        let c = &mut self.cols;
        let nm = unpack(name);
        c.user.extend_from_slice(&(nm.len() as u32).to_le_bytes());
        c.user.extend_from_slice(nm);
        c.day.extend_from_slice(&day.to_le_bytes());
        c.sessions.extend_from_slice(&sessions.to_le_bytes());
        c.events.extend_from_slice(&events.to_le_bytes());
        c.rev.extend_from_slice(&(rev as i64).to_le_bytes());
        c.has_cat.push(cat != NO_CAT);
        if cat != NO_CAT {
            c.cat.push(cat);
        }
        c.n += 1;
    }

    fn person(&mut self, evs: &[PRec], cks: &[CRec]) {
        let nday = self.nday;
        let ws = self.ws;
        let cbase = ws - WEEK_MS;
        let name = if evs[0].pkey >> 63 == 1 { evs[0].pkey & !(1 << 63) } else { evs[0].uptr };
        self.sess.clear();
        let (mut uday, mut us, mut ue, mut ur) = (-1i32, 0u32, 0u32, 0u64);
        let mut prev = u32::MAX;
        for e in evs {
            if prev == u32::MAX || (e.ts - prev) as i64 > GAP_MS {
                let start = ws + e.ts as i64;
                let day = (start.div_euclid(DAY_MS) - self.dbase) as i32;
                if day != uday {
                    if uday >= 0 {
                        self.flush_day(name, (uday as i64 + self.dbase) as i32, us, ue, ur);
                    }
                    uday = day;
                    us = 0;
                    ue = 0;
                    ur = 0;
                }
                us += 1;
                self.sess.push(Sess { start, day: day as u32, rev: 0, purch: false });
            }
            prev = e.ts;
            ue += 1;
            if e.ty() == 2 {
                let price = e.price();
                ur += price;
                let s = self.sess.last_mut().unwrap();
                s.rev += price;
                s.purch = true;
                let k = e.cat() as usize;
                if self.cat_sum[k] == 0 {
                    self.cat_touched.push(e.cat());
                }
                self.cat_sum[k] += price;
            }
        }
        self.flush_day(name, (uday as i64 + self.dbase) as i32, us, ue, ur);

        // credit
        let mut ci = 0;
        for s in &self.sess {
            while ci < cks.len() && cbase + cks[ci].ts as i64 <= s.start {
                ci += 1;
            }
            let mut camp = self.organic;
            if ci > 0 && s.start - (cbase + cks[ci - 1].ts as i64) <= WEEK_MS {
                let cts = cks[ci - 1].ts;
                let mut best = ci - 1;
                let mut j = ci - 1;
                while j > 0 && cks[j - 1].ts == cts {
                    j -= 1;
                    if unpack(cks[j].cid) < unpack(cks[best].cid) {
                        best = j;
                    }
                }
                camp = cks[best].camp;
            }
            let a = &mut self.camp[camp as usize * nday + s.day as usize];
            a[0] += 1;
            a[1] += s.purch as u64;
            a[2] += s.rev;
        }
        if cks.is_empty() {
            return;
        }

        // assists
        let n = self.sess.len();
        let mut p = 0;
        for d in 0..=nday + 1 {
            while p < n && (self.sess[p].day as usize) < d {
                p += 1;
            }
            self.dfirst[d] = p;
        }
        let (mut lo, mut hi) = (0usize, 0usize);
        for c in cks {
            let cts = cbase + c.ts as i64;
            while lo < n && self.sess[lo].start < cts {
                lo += 1;
            }
            while hi < n && self.sess[hi].start <= cts + WEEK_MS {
                hi += 1;
            }
            let k = c.camp as usize;
            let a = lo.max(self.covered[k] as usize);
            if a < hi {
                if self.covered[k] == 0 {
                    self.touched.push(c.camp);
                }
                self.covered[k] = hi as u32;
                let (da, db) = (self.sess[a].day as usize, self.sess[hi - 1].day as usize);
                for d in da..=db {
                    let l = a.max(self.dfirst[d]);
                    let h = hi.min(self.dfirst[d + 1]);
                    if h > l {
                        self.camp[k * nday + d][3] += (h - l) as u64;
                    }
                }
            }
        }
        for &k in &self.touched {
            self.covered[k as usize] = 0;
        }
        self.touched.clear();
    }
}
