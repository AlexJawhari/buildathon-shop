//! Parsing of event and click lines into partitioned records.

use crate::links::Links;
use crate::util::*;

pub const P1: usize = 1024; // event-id partitions (dedup)
pub const P2: usize = 1024; // person partitions (sessionize)
pub const WEEK_MS: i64 = 7 * DAY_MS;
pub const INVALID: u8 = 255;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct EvRec {
    pub h: u64,
    pub pkey: u64,
    pub uptr: u64,
    pub ts: i64,
    pub ingest: u32,
    pub chk: u32,
    pub price: u32,
    pub cat: u16,
    pub ty: u8, // 0 view, 1 cart, 2 purchase, INVALID
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct PRec {
    pub pkey: u64,
    pub ts: i64,
    pub uptr: u64,
    pub price: u32,
    pub cat: u16,
    pub ty: u8,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct CRec {
    pub pkey: u64,
    pub ts: i64,
    pub cid: u64,
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
    pub unknown_cat: u16,
}

pub struct Local {
    pub ev: Vec<Vec<EvRec>>,
    pub ck: Vec<Vec<CRec>>,
}
impl Local {
    pub fn new() -> Self {
        Local { ev: (0..P1).map(|_| Vec::new()).collect(), ck: (0..P2).map(|_| Vec::new()).collect() }
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
    let mut r = EvRec {
        h,
        pkey: 0,
        uptr: 0,
        ts: 0,
        ingest: match ing {
            Some(ms) => (ms / 1000).clamp(0, u32::MAX as i64) as u32,
            None => u32::MAX,
        },
        chk,
        price: 0,
        cat: 0,
        ty: INVALID,
    };
    'v: {
        let (Some(ts), Some(ing)) = (parse_ts(fl(2)), ing) else { break 'v };
        if ts < cx.ws || ts >= cx.we || ing.saturating_sub(ts) > DAY_MS {
            break 'v;
        }
        let user = fl(1);
        let ty = match fl(4) {
            b"view" => 0,
            b"cart" => 1,
            b"purchase" => 2,
            _ => break 'v,
        };
        let prod = fl(5);
        if user.is_empty() || prod.is_empty() {
            break 'v;
        }
        if ty == 2 {
            let Some(p) = parse_price(fl(6)) else { break 'v };
            r.price = p;
            r.cat = cx.prods.get(hash(prod)).map_or(cx.unknown_cat, |c| c as u16);
        }
        r.ts = ts;
        r.ty = ty;
        r.pkey = cx.links.person(hash(user));
        r.uptr = pack(user);
    }
    out.ev[(h >> 54) as usize].push(r);
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
    out.ck[bucket(pkey)].push(CRec { pkey, ts, cid: pack(cid), camp });
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
