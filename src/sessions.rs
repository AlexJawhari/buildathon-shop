//! Per-person sessionization, click credit/assists and aggregation.

use crate::ingest::*;
use crate::util::*;

const GAP_MS: i64 = 30 * 60 * 1000;
pub const NO_CAT: u16 = u16::MAX;

pub struct UserRow {
    pub name: u64,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub rev: u64,
    pub cat: u16,
}

struct Sess {
    start: i64,
    day: u32,
    rev: u64,
    purch: bool,
}

pub struct State {
    pub dbase: i64,
    pub nday: usize,
    pub organic: u32,
    pub camp: Vec<[u64; 4]>, // [(campaign, day)] -> sessions, purchasing, revenue, assisted
    pub rows: Vec<UserRow>,
    sess: Vec<Sess>,
    dfirst: Vec<usize>,
    covered: Vec<u32>,
    touched: Vec<u32>,
    cat_sum: Vec<u64>,
    cat_touched: Vec<u16>,
}

impl State {
    pub fn new(dbase: i64, nday: usize, ncamp: usize, ncat: usize) -> State {
        State {
            dbase,
            nday,
            organic: ncamp as u32,
            camp: vec![[0; 4]; (ncamp + 1) * nday],
            rows: vec![],
            sess: vec![],
            dfirst: vec![0; nday + 2],
            covered: vec![0; ncamp + 1],
            touched: vec![],
            cat_sum: vec![0; ncat],
            cat_touched: vec![],
        }
    }

    pub fn bucket(&mut self, evs: &mut [PRec], cks: &mut [CRec]) {
        evs.sort_unstable_by_key(|r| (r.pkey, r.ts));
        cks.sort_unstable_by_key(|r| (r.pkey, r.ts));
        let (mut i, mut c) = (0, 0);
        while i < evs.len() {
            let pk = evs[i].pkey;
            let mut j = i + 1;
            while j < evs.len() && evs[j].pkey == pk {
                j += 1;
            }
            while c < cks.len() && cks[c].pkey < pk {
                c += 1;
            }
            let mut d = c;
            while d < cks.len() && cks[d].pkey == pk {
                d += 1;
            }
            self.person(&evs[i..j], &cks[c..d]);
            i = j;
            c = d;
        }
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
        self.rows.push(UserRow { name, day, sessions, events, rev, cat });
    }

    fn person(&mut self, evs: &[PRec], cks: &[CRec]) {
        let nday = self.nday;
        let name = if evs[0].pkey >> 63 == 1 { evs[0].pkey & !(1 << 63) } else { evs[0].uptr };
        self.sess.clear();
        // user-day accumulator
        let (mut uday, mut us, mut ue, mut ur) = (-1i32, 0u32, 0u32, 0u64);
        let mut prev = i64::MIN;
        for e in evs {
            if prev == i64::MIN || e.ts - prev > GAP_MS {
                let day = (e.ts.div_euclid(DAY_MS) - self.dbase) as i32;
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
                self.sess.push(Sess { start: e.ts, day: day as u32, rev: 0, purch: false });
            }
            prev = e.ts;
            ue += 1;
            if e.ty == 2 {
                ur += e.price as u64;
                let s = self.sess.last_mut().unwrap();
                s.rev += e.price as u64;
                s.purch = true;
                let k = e.cat as usize;
                if self.cat_sum[k] == 0 {
                    self.cat_touched.push(e.cat);
                }
                self.cat_sum[k] += e.price as u64;
            }
        }
        self.flush_day(name, (uday as i64 + self.dbase) as i32, us, ue, ur);

        // credit
        let mut ci = 0;
        for s in &self.sess {
            while ci < cks.len() && cks[ci].ts <= s.start {
                ci += 1;
            }
            let mut camp = self.organic;
            if ci > 0 && s.start - cks[ci - 1].ts <= WEEK_MS {
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
            while lo < n && self.sess[lo].start < c.ts {
                lo += 1;
            }
            while hi < n && self.sess[hi].start <= c.ts + WEEK_MS {
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
