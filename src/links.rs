//! Linked IDs: a lock-free hash table of every ID in a valid link line, joined by a
//! lock-free union-find whose root is always the byte-wise smallest string. After `assign`, every slot holds a
//! dense person id (+1); IDs first seen in events/clicks are added the same way, so one probe maps any user id to a person.
//! `names[pid]` is the packed (ptr,len) of the person's name.

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
    names: Box<[AtomicU64]>,
    npid: AtomicU32,
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
    /// `max_ids`: upper bound on distinct user IDs (links, events and clicks together).
    pub fn new(max_ids: usize) -> Links {
        let cap = (max_ids * 23 / 20 + 16).next_power_of_two();
        Links { slots: zeroed(cap), parent: zeroed(cap), mask: cap - 1, names: zeroed(cap), npid: AtomicU32::new(0) }
    }

    pub fn npid(&self) -> usize {
        self.npid.load(Relaxed) as usize
    }

    pub fn name(&self, pid: usize) -> u64 {
        self.names[pid].load(Relaxed)
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

    /// Step 1 (parallel over slot ranges): give every union-find root a person id.
    pub fn assign_roots(&self, range: std::ops::Range<usize>, tmp: &[AtomicU32]) {
        for i in range {
            if self.slots[i].k.load(Relaxed) != 0 && self.parent[i].load(Relaxed) == 0 {
                let pid = self.npid.fetch_add(1, Relaxed);
                self.names[pid as usize].store(self.slots[i].v.load(Relaxed), Relaxed);
                tmp[i].store(pid + 1, Relaxed);
            }
        }
    }
    /// Step 2 (after step 1 finished everywhere): every slot takes its root's person id.
    pub fn assign_all(&self, range: std::ops::Range<usize>, tmp: &[AtomicU32]) {
        for i in range {
            if self.slots[i].k.load(Relaxed) != 0 {
                let r = self.find(i);
                self.slots[i].v.store(tmp[r].load(Relaxed) as u64, Relaxed);
            }
        }
    }
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }
    pub fn tmp(&self) -> Box<[AtomicU32]> {
        zeroed(self.slots.len())
    }

    #[inline(always)]
    pub fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.slots.as_ptr().add((h as usize) & self.mask) as *const i8, _MM_HINT_T0);
        }
    }

    /// Person id of a user id (hash `h`, bytes `name`), adding the ID as a new person when unseen.
    #[inline(always)]
    pub fn pid(&self, h: u64, name: &[u8]) -> u32 {
        let mut i = (h as usize) & self.mask;
        let mut probes = 0usize;
        loop {
            let s = &self.slots[i];
            let k = s.k.load(Relaxed);
            if k == h {
                return Self::wait(s);
            }
            if k == 0 {
                match s.k.compare_exchange(0, h, AcqRel, Acquire) {
                    Ok(_) => {
                        let pid = self.npid.fetch_add(1, Relaxed);
                        self.names[pid as usize].store(pack(name), Relaxed);
                        s.v.store(pid as u64 + 1, Release);
                        return pid;
                    }
                    Err(cur) if cur == h => return Self::wait(s),
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
            probes += 1;
            assert!(probes <= self.mask, "user id table full");
        }
    }

    #[inline(always)]
    fn wait(s: &Slot) -> u32 {
        loop {
            let v = s.v.load(Acquire);
            if v != 0 {
                return (v - 1) as u32;
            }
            std::hint::spin_loop();
        }
    }
}
