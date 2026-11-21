//! Structural scan of a chunk of lines: SIMD index of commas, newlines and quotes,
//! then a line iterator that applies the CSV dialect rules from the index alone.

use crate::util::Chunk;

#[derive(Default)]
pub struct Index {
    pub c: Vec<u32>,    // commas
    pub n: Vec<u32>,    // newlines (plus a virtual one at the end of a file without final '\n')
    pub qm: Vec<u64>,   // quote bitmask per 64-byte block
    pub qpre: Vec<u32>, // quotes before each block
    pub region: &'static [u8],
}

/// The bytes of whole data lines that start in the chunk (header line excluded).
pub fn region(c: Chunk) -> &'static [u8] {
    let (data, a, b) = c;
    let p = if a == 0 {
        match memchr::memchr(b'\n', data) {
            Some(i) => i + 1,
            None => return &[],
        }
    } else if data[a - 1] == b'\n' {
        a
    } else {
        match memchr::memchr(b'\n', &data[a..]) {
            Some(i) => a + i + 1,
            None => return &[],
        }
    };
    let e = if b >= data.len() {
        data.len()
    } else if data[b - 1] == b'\n' {
        b
    } else {
        memchr::memchr(b'\n', &data[b..]).map_or(data.len(), |i| b + i + 1)
    };
    if p >= e {
        &[]
    } else {
        &data[p..e]
    }
}

/// Write the positions of the set bits of `m`; `k` writes are unconditional.
#[inline(always)]
unsafe fn emit<const K: usize>(out: *mut u32, base: u32, mut m: u64) -> usize {
    let cnt = m.count_ones() as usize;
    for i in 0..K {
        *out.add(i) = base + m.trailing_zeros();
        m &= m.wrapping_sub(1);
    }
    for i in K..cnt {
        *out.add(i) = base + m.trailing_zeros();
        m &= m.wrapping_sub(1);
    }
    cnt
}

#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn block(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let cm = _mm256_set1_epi8(b',' as i8);
    let nm = _mm256_set1_epi8(b'\n' as i8);
    let qm = _mm256_set1_epi8(b'"' as i8);
    let m = |v, k| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, k)) as u32 as u64;
    (m(a, cm) | m(b, cm) << 32, m(a, nm) | m(b, nm) << 32, m(a, qm) | m(b, qm) << 32)
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
unsafe fn block(p: *const u8) -> (u64, u64, u64) {
    let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        match *p.add(i) {
            b',' => c |= 1 << i,
            b'\n' => n |= 1 << i,
            b'"' => q |= 1 << i,
            _ => {}
        }
    }
    (c, n, q)
}

impl Index {
    pub fn build(&mut self, region: &'static [u8]) {
        self.region = region;
        let len = region.len();
        self.c.clear();
        self.n.clear();
        self.qm.clear();
        self.qpre.clear();
        self.c.reserve(len / 8 + 128);
        self.n.reserve(len / 32 + 128);
        self.qm.reserve(len / 64 + 2);
        self.qpre.reserve(len / 64 + 2);
        let mut off = 0;
        let mut tail = [0u8; 64];
        let mut qrun = 0u32;
        unsafe {
            while off < len {
                let p = if off + 64 <= len {
                    region.as_ptr().add(off)
                } else {
                    tail[..len - off].copy_from_slice(&region[off..]);
                    tail[len - off..].fill(0);
                    tail.as_ptr()
                };
                let (c, n, q) = block(p);
                let base = off as u32;
                if self.c.capacity() - self.c.len() < 80 {
                    self.c.reserve(len / 8 + 128);
                }
                if self.n.capacity() - self.n.len() < 80 {
                    self.n.reserve(len / 32 + 128);
                }
                let k = emit::<8>(self.c.as_mut_ptr().add(self.c.len()), base, c);
                self.c.set_len(self.c.len() + k);
                let k = emit::<2>(self.n.as_mut_ptr().add(self.n.len()), base, n);
                self.n.set_len(self.n.len() + k);
                self.qm.push(q);
                self.qpre.push(qrun);
                qrun += q.count_ones();
                off += 64;
            }
        }
        self.qm.push(0);
        self.qpre.push(qrun);
        if region[len - 1] != b'\n' {
            self.n.push(len as u32);
        }
    }

    #[inline(always)]
    fn qcount(&self, pos: u32) -> u32 {
        let w = (pos >> 6) as usize;
        unsafe { *self.qpre.get_unchecked(w) + (*self.qm.get_unchecked(w) & ((1u64 << (pos & 63)) - 1)).count_ones() }
    }
}

/// Iterates the lines of an index. `NF` is the number of fields per line.
#[derive(Clone, Copy)]
pub struct Lines {
    ci: usize,
    ni: usize,
}

pub type Fields<const NF: usize> = [(u32, u32); NF];

impl Lines {
    pub fn new() -> Lines {
        Lines { ci: 0, ni: 0 }
    }

    /// None at the end; Some(None) for a malformed line; Some(Some(fields)) with
    /// quotes already stripped from the field bounds.
    #[inline(always)]
    pub fn next<const NF: usize>(&mut self, ix: &Index) -> Option<Option<Fields<NF>>> {
        let nl = *ix.n.get(self.ni)?;
        let ls = if self.ni == 0 { 0 } else { ix.n[self.ni - 1] + 1 };
        self.ni += 1;
        let c = &ix.c[..];
        let cb = self.ci;
        // exactly NF-1 commas before nl: the last one is before nl, the one after is past it
        let ok = cb + NF - 1 <= c.len()
            && (cb + NF - 1 == c.len() || unsafe { *c.get_unchecked(cb + NF - 1) } > nl)
            && unsafe { *c.get_unchecked(cb + NF - 2) } < nl;
        if !ok {
            let mut e = cb;
            while e < c.len() && c[e] < nl {
                e += 1;
            }
            self.ci = e;
            return Some(None);
        }
        self.ci = cb + NF - 1;
        let mut f: Fields<NF> = [(0, 0); NF];
        unsafe {
            for k in 0..NF {
                f[k].0 = if k == 0 { ls } else { *c.get_unchecked(cb + k - 1) + 1 };
                f[k].1 = if k < NF - 1 { *c.get_unchecked(cb + k) } else { nl };
            }
        }
        let nq = ix.qcount(nl) - ix.qcount(ls);
        if nq != 0 {
            let data = ix.region;
            let mut quoted = 0;
            for x in f.iter_mut() {
                if x.1 > x.0 && data[x.0 as usize] == b'"' {
                    quoted += 1;
                    if x.1 - x.0 < 2 || data[x.1 as usize - 1] != b'"' {
                        return Some(None);
                    }
                    x.0 += 1;
                    x.1 -= 1;
                }
            }
            if nq != 2 * quoted {
                return Some(None);
            }
        }
        Some(Some(f))
    }
}

pub static NOPOP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static MODE: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
const AHEAD: usize = 12;

/// Run `f` on every well-formed line of the chunk. `pre` gets the user-id bytes of the
/// line AHEAD lines ahead (assuming well-formed lines in between) to prefetch for.
#[inline(always)]
pub fn run_chunk<const NF: usize>(
    c: Chunk,
    ix: &mut Index,
    mut pre: impl FnMut(&'static [u8]),
    mut f: impl FnMut(&'static [u8], &Fields<NF>),
) {
    let r = region(c);
    if r.is_empty() {
        return;
    }
    if !NOPOP.load(std::sync::atomic::Ordering::Relaxed) {
        populate(r);
    }
    ix.build(r);
    if MODE.load(std::sync::atomic::Ordering::Relaxed) == 5 {
        return;
    }
    let mut it = Lines::new();
    while let Some(line) = it.next::<NF>(ix) {
        if NF > 2 {
            let k = it.ci + (AHEAD - 1) * (NF - 1);
            if k + 1 < ix.c.len() {
                let (mut s, mut e) = (ix.c[k] as usize + 1, ix.c[k + 1] as usize);
                if e > s + 1 && r[s] == b'"' {
                    s += 1;
                    e -= 1;
                }
                if e > s {
                    pre(&r[s..e]);
                }
            }
        }
        if let Some(fl) = &line {
            f(r, fl);
        }
    }
}

/// Map the chunk's pages in one call instead of faulting them in 64 KB at a time.
fn populate(r: &[u8]) {
    const MADV_POPULATE_READ: i32 = 22;
    let a = r.as_ptr() as usize & !4095;
    let e = (r.as_ptr() as usize + r.len() + 4095) & !4095;
    let rc = unsafe { libc::madvise(a as *mut _, e - a, MADV_POPULATE_READ) };
    if rc != 0 && std::env::var_os("SHOP_DBG").is_some() {
        eprintln!("populate failed: {}", std::io::Error::last_os_error());
    }
}
