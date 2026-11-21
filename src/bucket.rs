//! Append-only bucket storage carved from a per-thread, pre-faulted arena.
//! Fresh pages are populated in one madvise call instead of one page fault each.

const SH: usize = 8;
const BLK: usize = 1 << SH; // records per block

pub struct Arena {
    base: *mut u8,
    cap: usize,
    used: usize,
}
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    pub fn new(cap: usize) -> Arena {
        let cap = (cap + 4095) & !4095;
        if cap == 0 {
            return Arena { base: std::ptr::null_mut(), cap: 0, used: 0 };
        }
        unsafe {
            let p = libc::mmap(
                std::ptr::null_mut(),
                cap,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            );
            if p == libc::MAP_FAILED {
                return Arena { base: std::ptr::null_mut(), cap: 0, used: 0 };
            }
            libc::madvise(p, cap, libc::MADV_HUGEPAGE);
            let rc = libc::madvise(p, cap, 23); // MADV_POPULATE_WRITE; ignored by older kernels
            if rc != 0 && std::env::var_os("SHOP_DBG").is_some() {
                eprintln!("arena populate failed: {}", std::io::Error::last_os_error());
            }
            Arena { base: p as *mut u8, cap, used: 0 }
        }
    }

    /// Give the arena's pages back to the kernel (callable from any thread once nothing reads it).
    pub fn release(&self) {
        if self.cap > 0 {
            unsafe { libc::munmap(self.base as *mut _, self.cap) };
        }
    }

    fn alloc<T>(&mut self, n: usize) -> *mut T {
        let bytes = (n * std::mem::size_of::<T>() + 63) & !63;
        if self.used + bytes <= self.cap {
            let p = unsafe { self.base.add(self.used) };
            self.used += bytes;
            p as *mut T
        } else {
            unsafe { std::alloc::alloc(std::alloc::Layout::from_size_align(bytes, 64).unwrap()) as *mut T }
        }
    }
}

pub struct Bucket<T> {
    blocks: Vec<*mut T>,
    len: usize,
}
unsafe impl<T> Send for Bucket<T> {}
unsafe impl<T> Sync for Bucket<T> {}

impl<T: Copy> Bucket<T> {
    pub fn new() -> Self {
        Bucket { blocks: Vec::new(), len: 0 }
    }
    pub fn len(&self) -> usize {
        self.len
    }
    #[inline(always)]
    pub fn push(&mut self, v: T, arena: &mut Arena) {
        let i = self.len;
        if i & (BLK - 1) == 0 {
            self.blocks.push(arena.alloc::<T>(BLK));
        }
        unsafe { self.blocks.get_unchecked(i >> SH).add(i & (BLK - 1)).write(v) };
        self.len = i + 1;
    }
    #[inline(always)]
    pub fn ptr(&self, i: usize) -> *mut T {
        unsafe { self.blocks.get_unchecked(i >> SH).add(i & (BLK - 1)) }
    }
    pub fn get(&self, i: usize) -> &T {
        unsafe { &*self.ptr(i) }
    }
    /// The contents as a sequence of slices.
    pub fn slices(&self) -> impl Iterator<Item = &[T]> {
        let len = self.len;
        self.blocks.iter().enumerate().map(move |(b, &p)| {
            let n = (len - (b << SH)).min(BLK);
            unsafe { std::slice::from_raw_parts(p, n) }
        })
    }
}
