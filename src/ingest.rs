//! Parsing of event and click lines into partitioned records.

use crate::links::Links;
use crate::util::*;

pub const P1: usize = 1024; // event-id partitions (dedup)
pub const P2: usize = 1024; // person partitions (sessionize)
pub const WEEK_MS: i64 = 7 * DAY_MS;
pub const DEAD: u32 = u32::MAX; // pc value of a record killed by dedup

/// Event kept for sessionizing. `ts` is ms since the window start; `pc` packs
/// price (24 bits), type (2 bits: 0 view, 1 cart, 2 purchase) and category (6 bits).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct PRec {
    pub pkey: u64,
    pub uptr: u64,
    pub ts: u32,
    pub pc: u32,
}

impl PRec {
    #[inline(always)]
    pub fn price(&self) -> u64 {
        (self.pc & 0xFF_FFFF) as u64
    }
    #[inline(always)]
    pub fn ty(&self) -> u32 {
        (self.pc >> 24) & 3
    }
    #[inline(always)]
    pub fn cat(&self) -> u16 {
        (self.pc >> 26) as u16
    }
}

/// Dedup record, one per well-formed event line. meta: bits 0-23 index in the
/// (thread, bucket) vector, 24-33 bucket, 34-37 thread, 38 invalid, 39-63 check.
#[derive(Clone, Copy)]
#[repr(C, packed)]
pub struct Slim {
    pub h: u64,
    pub meta: u64,
    pub ingest: u32,
}
pub const META_INVALID: u64 = 1 << 38;

/// Click; `ts` is ms since window start minus 7 days.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CRec {
    pub pkey: u64,
    pub cid: u64,
    pub ts: u32,
    pub camp: u32,
}

#[inline(always)]
pub fn bucket(pkey: u64) -> usize {
    (pkey.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 54) as usize
}

/// Open-addressing hash -> u32 map, built once, then read-only.
pub struct SmallMap {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
}
impl SmallMap {
    pub fn new(n: usize) -> Self {
        let cap = (n * 2 + 16).next_power_of_two();
        SmallMap { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1 }
    }
    pub fn insert(&mut self, h: u64, v: u32) {
        let mut i = h as usize & self.mask;
        while self.keys[i] != 0 && self.keys[i] != h {
            i = (i + 1) & self.mask;
        }
        self.keys[i] = h;
        self.vals[i] = v;
    }
    #[inline(always)]
    pub fn get(&self, h: u64) -> Option<u32> {
        let mut i = h as usize & self.mask;
        loop {
            let k = self.keys[i];
            if k == h {
                return Some(self.vals[i]);
            }
            if k == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

pub struct Cx {
    pub ws: i64,
    pub we: i64,
    pub links: Links,
    pub prods: SmallMap,
    pub camps: SmallMap,
    pub unknown_cat: u32,
}

pub struct Local {
    pub t: usize,
    pub pv: Vec<Vec<PRec>>,
    pub sl: Vec<Vec<Slim>>,
    pub ck: Vec<Vec<CRec>>,
}
impl Local {
    pub fn new(t: usize, est_ev: usize, est_ck: usize) -> Self {
        let e = est_ev / P2 + 16;
        let c = est_ck / P2 + 16;
        Local {
            t,
            pv: (0..P2).map(|_| Vec::with_capacity(e)).collect(),
            sl: (0..P1).map(|_| Vec::with_capacity(e)).collect(),
            ck: (0..P2).map(|_| Vec::with_capacity(c)).collect(),
        }
    }
}

#[inline(always)]
pub fn event_line(line: &'static [u8], cx: &Cx, out: &mut Local) {
    let mut f = [(0usize, 0usize); 7];
    if !split(line, &mut f) {
        return;
    }
    let fl = |i: usize| &line[f[i].0..f[i].1];
    let id = fl(0);
    if id.is_empty() {
        return;
    }
    let (h, chk) = hash_chk(id);
    let ing = parse_ts(fl(3));
    let ingest = match ing {
        Some(ms) => (ms / 1000).clamp(0, u32::MAX as i64 - 1) as u32,
        None => u32::MAX,
    };
    let mut meta = ((chk >> 7) as u64) << 39 | META_INVALID;
    'v: {
        let (Some(ts), Some(ing)) = (parse_ts(fl(2)), ing) else { break 'v };
        if ts < cx.ws || ts >= cx.we || ing.saturating_sub(ts) > DAY_MS {
            break 'v;
        }
        let user = fl(1);
        let ty = match fl(4) {
            b"view" => 0u32,
            b"cart" => 1,
            b"purchase" => 2,
            _ => break 'v,
        };
        let prod = fl(5);
        if user.is_empty() || prod.is_empty() {
            break 'v;
        }
        let mut pc = ty << 24;
        if ty == 2 {
            let Some(p) = parse_price(fl(6)) else { break 'v };
            pc |= p | (cx.prods.get(hash(prod)).unwrap_or(cx.unknown_cat) << 26);
        }
        let pkey = cx.links.person(hash(user));
        let b = bucket(pkey);
        let v = &mut out.pv[b];
        let i = v.len();
        assert!(i < 1 << 24);
        v.push(PRec { pkey, uptr: pack(user), ts: (ts - cx.ws) as u32, pc });
        meta = (meta & !META_INVALID) | i as u64 | (b as u64) << 24 | (out.t as u64) << 34;
    }
    out.sl[(h >> 54) as usize].push(Slim { h, meta, ingest });
}

#[inline(always)]
pub fn click_line(line: &'static [u8], cx: &Cx, out: &mut Local) {
    let mut f = [(0usize, 0usize); 4];
    if !split(line, &mut f) {
        return;
    }
    let fl = |i: usize| &line[f[i].0..f[i].1];
    let (cid, user) = (fl(0), fl(1));
    if cid.is_empty() || user.is_empty() {
        return;
    }
    let Some(camp) = cx.camps.get(hash(fl(2))) else { return };
    let Some(ts) = parse_ts(fl(3)) else { return };
    if ts < cx.ws - WEEK_MS || ts >= cx.we {
        return;
    }
    let pkey = cx.links.person(hash(user));
    out.ck[bucket(pkey)].push(CRec { pkey, cid: pack(cid), ts: (ts - (cx.ws - WEEK_MS)) as u32, camp });
}

/// Returns the number of lines that were valid link lines (both sides non-empty).
#[inline(always)]
pub fn link_line(line: &'static [u8], cx: &Cx) {
    let mut f = [(0usize, 0usize); 2];
    if !split(line, &mut f) {
        return;
    }
    let (a, b) = (&line[f[0].0..f[0].1], &line[f[1].0..f[1].1]);
    if a.is_empty() || b.is_empty() {
        return;
    }
    cx.links.add(a, b);
}
