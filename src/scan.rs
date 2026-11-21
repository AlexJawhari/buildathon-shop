//! Line scanner: for each line of a chunk, one pass of AVX2 compares finds the newline, the commas and the
//! quotes, applies the CSV dialect rules, and yields field bounds. No intermediate index.

use crate::util::{split, Chunk};

pub type Fields<const NF: usize> = [(u32, u32); NF];

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


/// Scan the line starting at `p`. Returns (line length without '\n', fields or None if malformed).
/// `buf` must have at least 128 readable bytes at `p`; bytes at or after `avail` must not be '\n', ',' or '"'
/// (zero padding), and `avail` is the number of real bytes.
#[inline(always)]
unsafe fn scan_line<const NF: usize>(p: *const u8, avail: usize) -> (usize, Option<Fields<NF>>) {
    let (c0, n0, q0) = block(p);
    let (c1, n1, q1);
    let l;
    if n0 != 0 {
        l = n0.trailing_zeros() as usize;
        (c1, n1, q1) = (0, 0, 0);
    } else {
        let t = block(p.add(64));
        (c1, n1, q1) = t;
        if n1 != 0 {
            l = 64 + n1.trailing_zeros() as usize;
        } else {
            // no newline within 128 bytes: last line without '\n', or a long line
            return slow_line::<NF>(p, avail);
        }
    }
    let (cm0, cm1, qm0, qm1) = if l < 64 {
        let m = (1u64 << l) - 1;
        (c0 & m, 0, q0 & m, 0)
    } else {
        let m = if l == 64 { 0 } else { (1u64 << (l - 64)) - 1 };
        (c0, c1 & m, q0, q1 & m)
    };
    let _ = n1;
    if (cm0.count_ones() + cm1.count_ones()) as usize != NF - 1 {
        return (l, None);
    }
    let mut pos = [0u32; 16];
    let mut k = 0;
    let mut m = cm0;
    while m != 0 {
        pos[k] = m.trailing_zeros();
        m &= m - 1;
        k += 1;
    }
    m = cm1;
    while m != 0 {
        pos[k] = 64 + m.trailing_zeros();
        m &= m - 1;
        k += 1;
    }
    let mut f: Fields<NF> = [(0, 0); NF];
    for i in 0..NF {
        f[i].0 = if i == 0 { 0 } else { pos[i - 1] + 1 };
        f[i].1 = if i < NF - 1 { pos[i] } else { l as u32 };
    }
    let nq = qm0.count_ones() + qm1.count_ones();
    if nq != 0 && !strip_quotes::<NF>(p, &mut f, nq) {
        return (l, None);
    }
    (l, Some(f))
}

#[inline(always)]
unsafe fn strip_quotes<const NF: usize>(p: *const u8, f: &mut Fields<NF>, nq: u32) -> bool {
    let mut quoted = 0;
    for x in f.iter_mut() {
        if x.1 > x.0 && *p.add(x.0 as usize) == b'"' {
            quoted += 1;
            if x.1 - x.0 < 2 || *p.add(x.1 as usize - 1) != b'"' {
                return false;
            }
            x.0 += 1;
            x.1 -= 1;
        }
    }
    nq == 2 * quoted
}

/// Lines with no newline in 128 bytes: find it the slow way (or take the rest of the data).
#[cold]
#[inline(never)]
unsafe fn slow_line<const NF: usize>(p: *const u8, avail: usize) -> (usize, Option<Fields<NF>>) {
    let rest = std::slice::from_raw_parts(p, avail);
    let l = memchr::memchr(b'\n', rest).unwrap_or(avail);
    let mut f = [(0usize, 0usize); NF];
    if split::<NF>(&rest[..l], &mut f) {
        let mut g: Fields<NF> = [(0, 0); NF];
        for i in 0..NF {
            g[i] = (f[i].0 as u32, f[i].1 as u32);
        }
        (l, Some(g))
    } else {
        (l, None)
    }
}

const BATCH: usize = 16;

/// Run `f(line, fields, uh)` on every well-formed line of the chunk. `line` starts at the line's first byte.
/// If `PRE`, `uh` is the hash of field 1 (the user id) and its link-table slot is prefetched a batch ahead.
#[inline(always)]
pub fn run_chunk<const NF: usize, const PRE: bool>(
    c: Chunk,
    hash_pre: impl Fn(&'static [u8]) -> u64,
    prefetch: impl Fn(u64),
    mut f: impl FnMut(&'static [u8], &Fields<NF>, u64),
) {
    let r = region(c);
    if r.is_empty() {
        return;
    }
    if !NOPOP.load(std::sync::atomic::Ordering::Relaxed) {
        populate(r);
    }
    let len = r.len();
    let mut p = 0usize;
    // lines whose 128-byte window would run past the region go through a zero-padded copy
    let tail_start = len.saturating_sub(128);
    let mut batch: [(u32, Fields<NF>, u64); BATCH] = [(0, [(0, 0); NF], 0); BATCH];
    let mut bn;
    while p < tail_start {
        bn = 0;
        let mut q = p;
        while bn < BATCH && q < tail_start {
            let (l, fl) = unsafe { scan_line::<NF>(r.as_ptr().add(q), len - q) };
            if let Some(fl) = fl {
                let uh = if PRE {
                    let u = &r[q + fl[1].0 as usize..q + fl[1].1 as usize];
                    let h = hash_pre(u);
                    prefetch(h);
                    h
                } else {
                    0
                };
                batch[bn] = (q as u32, fl, uh);
                bn += 1;
            }
            q += l + 1;
        }
        for b in &batch[..bn] {
            f(&r[b.0 as usize..], &b.1, b.2);
        }
        p = q;
    }
    if p < len {
        // tail: copy into a padded buffer for scanning; fields are relative to the line start, so the real region is used for data
        let mut buf = [0u8; 384];
        let rem = len - p;
        buf[..rem].copy_from_slice(&r[p..]);
        let mut q = 0usize;
        while q < rem {
            let (l, fl) = unsafe { scan_line::<NF>(buf.as_ptr().add(q), rem - q) };
            if let Some(fl) = fl {
                let uh = if PRE { hash_pre(&r[p + q + fl[1].0 as usize..p + q + fl[1].1 as usize]) } else { 0 };
                f(&r[p + q..], &fl, uh);
            }
            q += l + 1;
        }
    }
}

pub static NOPOP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Map the chunk's pages in one call instead of faulting them in 64 KB at a time.
fn populate(r: &[u8]) {
    const MADV_POPULATE_READ: i32 = 22;
    let a = r.as_ptr() as usize & !4095;
    let e = (r.as_ptr() as usize + r.len() + 4095) & !4095;
    unsafe { libc::madvise(a as *mut _, e - a, MADV_POPULATE_READ) };
}
