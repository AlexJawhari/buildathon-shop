//! Byte-level helpers: hashing, CSV field splitting, timestamps, mmap.

use std::fs::File;
use std::os::unix::io::AsRawFd;

const K0: u64 = 0x9E37_79B9_7F4A_7C15;
const K1: u64 = 0xD6E8_FEB8_6659_FD93;
const K2: u64 = 0xA076_1D64_78BD_642F;
const K3: u64 = 0xE703_7ED1_A0B4_28DB;

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

#[inline(always)]
fn rd8(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}
#[inline(always)]
fn rd4(b: &[u8], i: usize) -> u64 {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as u64
}

/// 64-bit hash plus an independent 32-bit check. Never returns h == 0.
#[inline(always)]
pub fn hash_chk(b: &[u8]) -> (u64, u32) {
    let len = b.len();
    let (a, c) = if len >= 16 {
        let mut h = len as u64 ^ K0;
        let mut i = 0;
        while i + 16 < len {
            h = mum(rd8(b, i) ^ K1 ^ h, rd8(b, i + 8) ^ K2);
            i += 16;
        }
        (rd8(b, len - 16) ^ h, rd8(b, len - 8))
    } else if len >= 8 {
        (rd8(b, 0), rd8(b, len - 8))
    } else if len >= 4 {
        (rd4(b, 0), rd4(b, len - 4))
    } else if len > 0 {
        (((b[0] as u64) << 16) | ((b[len / 2] as u64) << 8) | b[len - 1] as u64, 0)
    } else {
        (0, 0)
    };
    let l = len as u64;
    let lm = l.wrapping_mul(K2);
    let h = mum(a ^ K0, c ^ K1 ^ lm);
    let chk = (mum(a ^ K3, c ^ K0 ^ lm.rotate_left(29)) >> 32) as u32;
    (h | 1, chk)
}

#[inline(always)]
pub fn hash(b: &[u8]) -> u64 {
    hash_chk(b).0
}

/// Split one line (no '\n') into exactly N fields following the CSV dialect:
/// a field is plain (no quote) or wrapped whole in quotes (no comma or quote inside).
/// Returns false for a malformed line. out holds (start, end) of each field's content.
#[inline(always)]
pub fn split<const N: usize>(line: &[u8], out: &mut [(usize, usize); N]) -> bool {
    let n = line.len();
    let mut i = 0;
    let mut f = 0;
    loop {
        if f == N {
            return false;
        }
        let (s, e);
        if i < n && line[i] == b'"' {
            let mut j = i + 1;
            while j < n && line[j] != b'"' {
                if line[j] == b',' {
                    return false;
                }
                j += 1;
            }
            if j >= n {
                return false;
            }
            s = i + 1;
            e = j;
            i = j + 1;
            if i < n && line[i] != b',' {
                return false;
            }
        } else {
            let mut j = i;
            while j < n && line[j] != b',' {
                if line[j] == b'"' {
                    return false;
                }
                j += 1;
            }
            s = i;
            e = j;
            i = j;
        }
        out[f] = (s, e);
        f += 1;
        if i >= n {
            break;
        }
        i += 1;
    }
    f == N
}

pub const DAY_MS: i64 = 86_400_000;

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let x = b[i].wrapping_sub(b'0');
    let y = b[i + 1].wrapping_sub(b'0');
    if x > 9 || y > 9 {
        None
    } else {
        Some((x * 10 + y) as i64)
    }
}

/// Parse a timestamp (F1, F2 or F3) to epoch milliseconds, None if bad.
#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 20 || n == 25 {
        if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
            return None;
        }
        let y = d2(b, 0)? * 100 + d2(b, 2)?;
        let mo = d2(b, 5)?;
        let d = d2(b, 8)?;
        let h = d2(b, 11)?;
        let mi = d2(b, 14)?;
        let s = d2(b, 17)?;
        if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || s > 59 {
            return None;
        }
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let dim = match mo {
            2 => 28 + leap as i64,
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        if d > dim {
            return None;
        }
        let mut ms = (days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s) * 1000;
        if n == 20 {
            if b[19] != b'Z' {
                return None;
            }
        } else {
            let sign = match b[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if b[22] != b':' {
                return None;
            }
            let oh = d2(b, 20)?;
            let om = d2(b, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            ms -= sign * (oh * 60 + om) * 60_000;
        }
        return Some(ms);
    }
    if n == 0 {
        return None;
    }
    let mut i = 0;
    while i < n && b[i] == b'0' {
        i += 1;
    }
    if n - i > 19 {
        for &c in &b[i..] {
            if c.wrapping_sub(b'0') > 9 {
                return None;
            }
        }
        return None;
    }
    let mut v: u64 = 0;
    for &c in &b[i..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.wrapping_mul(10).wrapping_add(d as u64);
    }
    // 19 digits can exceed i64 (or wrap u64 only past 20 digits, excluded above)
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

pub fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

/// Memory-map a file read-only; the mapping is never unmapped.
pub fn mmap(path: &std::path::Path) -> &'static [u8] {
    let f = File::open(path).unwrap_or_else(|e| panic!("open {path:?}: {e}"));
    let len = f.metadata().unwrap().len() as usize;
    if len == 0 {
        return &[];
    }
    unsafe {
        let p = libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0);
        assert!(p != libc::MAP_FAILED, "mmap {path:?}");
        std::slice::from_raw_parts(p as *const u8, len)
    }
}

/// All `*.csv` files directly in `dir`, sorted, mapped.
pub fn csv_files(dir: &std::path::Path) -> Vec<&'static [u8]> {
    let mut v: Vec<_> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v.iter().map(|p| mmap(p)).collect()
}

pub type Chunk = (&'static [u8], usize, usize);

/// Split files into ~`size`-byte chunks. Lines belong to the chunk they start in.
pub fn chunks(files: &[&'static [u8]], size: usize) -> Vec<Chunk> {
    let mut out = vec![];
    for &f in files {
        let mut a = 0;
        loop {
            let b = (a + size).min(f.len());
            out.push((f, a, b));
            if b >= f.len() {
                break;
            }
            a = b;
        }
    }
    out
}

/// Call `f` for every data line starting in [a, b) (skipping the header line of the file).
#[inline(always)]
pub fn for_lines(c: Chunk, mut f: impl FnMut(&'static [u8])) {
    let (data, a, b) = c;
    let mut p = a;
    if a == 0 {
        match memchr::memchr(b'\n', data) {
            Some(i) => p = i + 1,
            None => return,
        }
    } else if data[a - 1] != b'\n' {
        match memchr::memchr(b'\n', &data[a..]) {
            Some(i) => p = a + i + 1,
            None => return,
        }
    }
    while p < b {
        let end = match memchr::memchr(b'\n', &data[p..]) {
            Some(i) => p + i,
            None => data.len(),
        };
        f(&data[p..end]);
        p = end + 1;
    }
}

/// Pack a string's address and length (pointer < 2^47) into one word.
#[inline(always)]
pub fn pack(s: &[u8]) -> u64 {
    s.as_ptr() as u64 | (s.len().min(0xFFFF) as u64) << 47
}

#[inline(always)]
pub fn unpack(v: u64) -> &'static [u8] {
    unsafe { std::slice::from_raw_parts((v & ((1 << 47) - 1)) as *const u8, ((v >> 47) & 0xFFFF) as usize) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn short_hashes_distinct() {
        let mut seen = std::collections::HashSet::new();
        for s in ["p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p10", "p11"] {
            assert!(seen.insert(hash(s.as_bytes())), "collision {s}");
        }
    }
}
