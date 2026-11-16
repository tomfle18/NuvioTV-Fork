//! A block can only be recycled by its last owner, including readers copying outside the store lock.
use crate::error::{Error, Result};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

pub const SEGMENT: usize = 64 * 1024;
pub const GLOBAL_LIMIT: usize = 64 * 1024 * 1024;
static GLOBAL_BYTES: Mutex<usize> = Mutex::new(0);
pub fn global_allocated() -> usize {
    *lock(&GLOBAL_BYTES)
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // State is never trusted again after an unwind; JNI invalidates the session on panic.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

struct PoolState {
    limit: usize,
    allocated: usize,
    free: Vec<Vec<u8>>,
}
pub struct Pool {
    state: Mutex<PoolState>,
}
impl Pool {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(PoolState {
                limit,
                allocated: 0,
                free: Vec::new(),
            }),
        })
    }
    pub fn allocate(self: &Arc<Self>) -> Result<Writable> {
        self.allocate_for(SEGMENT)
    }
    pub fn allocate_for(self: &Arc<Self>, minimum: usize) -> Result<Writable> {
        if minimum == 0 || minimum > SEGMENT {
            return Err(Error::Invalid);
        }
        let requested = minimum.next_power_of_two().clamp(4096, SEGMENT);
        let mut s = lock(&self.state);
        if let Some(index) = s
            .free
            .iter()
            .position(|b| b.capacity() >= requested && b.capacity() <= requested * 2)
        {
            let mut bytes = s.free.swap_remove(index);
            bytes.clear();
            return Ok(Writable {
                bytes: Some(bytes),
                pool: self.clone(),
            });
        }
        let mut global = lock(&GLOBAL_BYTES);
        // Free incompatible spare buffers before denying a critical read's allocation.
        for b in std::mem::take(&mut s.free) {
            s.allocated -= b.capacity();
            *global -= b.capacity();
        }
        if s.allocated.saturating_add(requested) > s.limit
            || global.saturating_add(requested) > GLOBAL_LIMIT
        {
            return Err(Error::Budget);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(requested)
            .map_err(|_| Error::Budget)?;
        // Account capacity, rather than logical length, including a possible allocator rounding.
        let capacity = bytes.capacity();
        if s.allocated + capacity > s.limit || *global + capacity > GLOBAL_LIMIT {
            return Err(Error::Budget);
        }
        s.allocated += capacity;
        *global += capacity;
        Ok(Writable {
            bytes: Some(bytes),
            pool: self.clone(),
        })
    }
    fn recycle(&self, bytes: Vec<u8>) {
        let mut s = lock(&self.state);
        if s.allocated <= s.limit && s.free.len() < 2 {
            s.free.push(bytes);
        } else {
            s.allocated -= bytes.capacity();
            *lock(&GLOBAL_BYTES) -= bytes.capacity();
        }
    }
    pub fn set_limit(&self, limit: usize) {
        let mut s = lock(&self.state);
        s.limit = limit;
        for b in std::mem::take(&mut s.free) {
            s.allocated -= b.capacity();
            *lock(&GLOBAL_BYTES) -= b.capacity();
        }
    }
    pub fn stats(&self) -> (usize, usize, usize) {
        let s = lock(&self.state);
        (s.allocated, s.free.iter().map(Vec::capacity).sum(), s.limit)
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        let s = lock(&self.state);
        *lock(&GLOBAL_BYTES) -= s.allocated;
    }
}
pub struct Writable {
    bytes: Option<Vec<u8>>,
    pool: Arc<Pool>,
}
impl Writable {
    pub fn extend(&mut self, bytes: &[u8]) -> Result<()> {
        let buffer = self.bytes.as_mut().ok_or(Error::Invalid)?;
        if buffer.len().saturating_add(bytes.len()) > buffer.capacity() {
            return Err(Error::Budget);
        }
        buffer.extend_from_slice(bytes);
        Ok(())
    }
    pub fn len(&self) -> usize {
        self.bytes.as_ref().map_or(0, Vec::len)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn seal(mut self) -> Result<Arc<Block>> {
        Ok(Arc::new(Block {
            bytes: self.bytes.take().ok_or(Error::Invalid)?,
            pool: self.pool.clone(),
        }))
    }
}
impl Drop for Writable {
    fn drop(&mut self) {
        if let Some(b) = self.bytes.take() {
            self.pool.recycle(b);
        }
    }
}
pub struct Block {
    bytes: Vec<u8>,
    pool: Arc<Pool>,
}
impl Block {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        self.pool.recycle(std::mem::take(&mut self.bytes));
    }
}
#[derive(Default)]
pub struct Store {
    blocks: BTreeMap<u64, Arc<Block>>,
}
impl Store {
    pub fn at(&self, offset: u64) -> Option<(u64, Arc<Block>)> {
        self.blocks
            .range(..=offset)
            .next_back()
            .filter(|(start, b)| offset - **start < b.bytes.len() as u64)
            .map(|(start, b)| (*start, b.clone()))
    }
    pub fn insert(&mut self, start: u64, block: Arc<Block>) -> Result<()> {
        let end = start
            .checked_add(block.bytes.len() as u64)
            .ok_or(Error::Invalid)?;
        if end == start {
            return Err(Error::Invalid);
        }
        // Keep intervals disjoint. Removing a cache entry never invalidates a reader's lease.
        if let Some((&key, old)) = self.blocks.range(..start).next_back() {
            if key.saturating_add(old.bytes.len() as u64) > start {
                self.blocks.remove(&key);
            }
        }
        let overlap: Vec<_> = self.blocks.range(start..end).map(|(k, _)| *k).collect();
        for k in overlap {
            self.blocks.remove(&k);
        }
        self.blocks.insert(start, block);
        Ok(())
    }
    pub fn clear(&mut self) {
        self.blocks.clear();
    }
    pub fn evict_before(&mut self, position: u64, protected: &[u64]) -> bool {
        let key = self
            .blocks
            .range(..position)
            .find(|(start, block)| {
                start.saturating_add(block.bytes.len() as u64) <= position
                    && Arc::strong_count(block) == 1
                    && !protected.iter().any(|p| {
                        *p >= **start && p.saturating_sub(**start) < block.bytes.len() as u64
                    })
            })
            .map(|(k, _)| *k);
        if let Some(key) = key {
            self.blocks.remove(&key);
            true
        } else {
            false
        }
    }
    pub fn evict(&mut self, protected: &[u64]) -> bool {
        let eligible = |(k, b): &(&u64, &Arc<Block>)| {
            Arc::strong_count(b) == 1
                && !protected
                    .iter()
                    .any(|p| *p >= **k && *p - **k < b.bytes.len() as u64)
        };
        // End candidates avoid scanning the entire cache for every network fragment.
        let first = self.blocks.iter().find(eligible);
        let last = self.blocks.iter().rev().find(eligible);
        let key = first
            .into_iter()
            .chain(last)
            .max_by_key(|(k, _)| {
                protected
                    .iter()
                    .map(|p| p.abs_diff(**k))
                    .min()
                    .unwrap_or(u64::MAX)
            })
            .map(|(k, _)| *k);
        if let Some(k) = key {
            self.blocks.remove(&k);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overlapping_intervals_never_hide_a_valid_suffix_or_recycle_a_lease() {
        let p = Pool::new(SEGMENT * 2);
        let mut old = p.allocate_for(32).unwrap();
        old.extend(&[7; 32]).unwrap();
        let old = old.seal().unwrap();
        let mut store = Store::default();
        store.insert(0, old.clone()).unwrap();
        let mut new = p.allocate_for(32).unwrap();
        new.extend(&[9; 32]).unwrap();
        store.insert(16, new.seal().unwrap()).unwrap();
        assert!(store.at(0).is_none());
        assert_eq!(store.at(47).unwrap().1.bytes(), &[9; 32]);
        assert_eq!(old.bytes(), &[7; 32]);
        assert_eq!(p.stats().0, 8192);
        p.set_limit(0);
        store.clear();
        assert_eq!(p.stats().0, 4096);
        drop(old);
        assert_eq!(p.stats().0, 0);
    }
    #[test]
    fn lease_survives_eviction_and_limit_reduction() {
        let p = Pool::new(SEGMENT * 2);
        let mut w = p.allocate().unwrap();
        w.extend(&[1, 2, 3]).unwrap();
        let mut s = Store::default();
        s.insert(1 << 34, w.seal().unwrap()).unwrap();
        let (_, read) = s.at((1 << 34) + 1).unwrap();
        s.clear();
        p.set_limit(0);
        assert_eq!(read.bytes(), &[1, 2, 3]);
        assert_eq!(p.stats().0, SEGMENT);
        drop(read);
        assert_eq!(p.stats().0, 0);
    }
    #[test]
    fn pool_is_included_in_budget() {
        let p = Pool::new(SEGMENT);
        drop(p.allocate().unwrap());
        assert_eq!(p.stats(), (SEGMENT, SEGMENT, SEGMENT));
        let held = p.allocate().unwrap();
        assert!(matches!(p.allocate(), Err(Error::Budget)));
        drop(held);
        p.set_limit(0);
        assert_eq!(p.stats().0, 0);
    }
}
