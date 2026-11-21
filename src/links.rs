//! Linked IDs: a lock-free hash table of every ID in a valid link line, joined by a
//! lock-free union-find whose root is always the byte-wise smallest string. After
//! `finish`, a lookup gives the packed (ptr,len) of the person's name.

use crate::util::*;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::*};

#[repr(C, align(16))]
struct Slot {
    k: AtomicU64,
    v: AtomicU64,
}

pub struct Links {
    slots: Box<[Slot]>,
    parent: Box<[AtomicU32]>, // 0 = root, else parent index + 1
    mask: usize,
}

fn zeroed<T>(n: usize) -> Box<[T]> {
    unsafe {
        let layout = std::alloc::Layout::array::<T>(n).unwrap();
        let p = std::alloc::alloc_zeroed(layout) as *mut T;
        assert!(!p.is_null());
        Box::from_raw(std::slice::from_raw_parts_mut(p, n))
    }
}

impl Links {
    pub fn new(max_ids: usize) -> Links {
        let cap = (max_ids * 5 / 4 + 16).next_power_of_two();
        Links { slots: zeroed(cap), parent: zeroed(cap), mask: cap - 1 }
    }

    fn insert(&self, h: u64, v: u64) -> usize {
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &self.slots[i];
            let k = s.k.load(Acquire);
            if k == h {
                return i;
            }
            if k == 0 {
                match s.k.compare_exchange(0, h, AcqRel, Acquire) {
                    Ok(_) => {
                        s.v.store(v, Release);
                        return i;
                    }
                    Err(cur) if cur == h => return i,
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
        }
    }

    fn val(&self, i: usize) -> u64 {
        loop {
            let v = self.slots[i].v.load(Acquire);
            if v != 0 {
                return v;
            }
            std::hint::spin_loop();
        }
    }

    fn find(&self, mut i: usize) -> usize {
        loop {
            let p = self.parent[i].load(Relaxed);
            if p == 0 {
                return i;
            }
            let pp = self.parent[p as usize - 1].load(Relaxed);
            if pp != 0 {
                self.parent[i].store(pp, Relaxed);
                i = pp as usize - 1;
            } else {
                i = p as usize - 1;
            }
        }
    }

    fn union(&self, a: usize, b: usize) {
        loop {
            let ra = self.find(a);
            let rb = self.find(b);
            if ra == rb {
                return;
            }
            let (child, root) = if unpack(self.val(ra)) < unpack(self.val(rb)) { (rb, ra) } else { (ra, rb) };
            if self.parent[child].compare_exchange(0, root as u32 + 1, AcqRel, Relaxed).is_ok() {
                return;
            }
        }
    }

    /// Add one link line (both sides non-empty, already validated).
    pub fn add(&self, a: &'static [u8], b: &'static [u8]) {
        let ia = self.insert(hash(a), pack(a));
        let ib = self.insert(hash(b), pack(b));
        if ia != ib {
            self.union(ia, ib);
        }
    }

    /// Replace each slot's value by its root's value (parallel over a slot range).
    pub fn flatten_read(&self, range: std::ops::Range<usize>, tmp: &[AtomicU64]) {
        for i in range {
            if self.slots[i].k.load(Relaxed) != 0 {
                let r = self.find(i);
                tmp[i].store(self.slots[r].v.load(Relaxed), Relaxed);
            }
        }
    }
    pub fn flatten_write(&self, range: std::ops::Range<usize>, tmp: &[AtomicU64]) {
        for i in range {
            if self.slots[i].k.load(Relaxed) != 0 {
                self.slots[i].v.store(tmp[i].load(Relaxed), Relaxed);
            }
        }
    }
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }
    pub fn tmp(&self) -> Box<[AtomicU64]> {
        zeroed(self.slots.len())
    }

    /// Person key for a user hash: linked persons are keyed by their name's packed
    /// address with the top bit set, unlinked IDs by their own hash with it clear.
    #[inline(always)]
    pub fn person(&self, h: u64) -> u64 {
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &self.slots[i];
            let k = s.k.load(Relaxed);
            if k == h {
                return s.v.load(Relaxed) | (1 << 63);
            }
            if k == 0 {
                return h & !(1 << 63);
            }
            i = (i + 1) & self.mask;
        }
    }
}
