use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::page_cache::FrameId;

#[derive(Debug)]
struct LRUKNode {
    i: FrameId,
    history: Vec<u64>,
    pin: u64,
}

impl LRUKNode {
    pub fn new(i: usize, ts: u64) -> Self {
        Self { i, history: vec![ts], pin: 0 }
    }

    pub fn get_k_distance(&self, k: usize) -> Option<u64> {
        let len = self.history.len();
        if len < k {
            return None;
        }

        let latest = self.history.last().unwrap();
        let kth = len - k;

        Some(latest - self.history[kth])
    }
}

#[derive(Default, Debug)]
pub struct LRUKReplacer {
    nodes: HashMap<FrameId, LRUKNode>,
    current_ts: u64,
    k: usize,
}

pub enum AccessType {
    Get,
    Scan,
}

impl LRUKReplacer {
    pub fn new(k: usize) -> Self {
        Self { k, ..Default::default() }
    }

    pub fn evict(&mut self) -> Option<FrameId> {
        let mut max: (FrameId, u64) = (0, 0);
        let mut single_access: Vec<&LRUKNode> = Vec::new();
        for (id, node) in &self.nodes {
            if node.pin != 0 {
                continue;
            }

            match node.get_k_distance(self.k) {
                Some(d) if d > max.1 => max = (*id, d),
                None => single_access.push(node),
                _ => {}
            };
        }

        if max.1 != 0 {
            return Some(max.0);
        }

        if single_access.is_empty() {
            return None;
        }

        // If multiple frames have less than k recorded accesses, choose the one with the
        // earliest timestamp to evict
        let mut earliest: (usize, u64) = (0, u64::MAX);
        for node in &single_access {
            match node.history.last() {
                Some(ts) if *ts < earliest.1 => earliest = (node.i, *ts),
                None => todo!(),
                _ => {}
            }
        }

        Some(earliest.0)
    }

    pub fn record_access(&mut self, i: FrameId, _access_type: AccessType) {
        match self.nodes.entry(i) {
            Entry::Occupied(mut node) => {
                node.get_mut().history.push(self.current_ts);
                self.current_ts += 1;
            }
            Entry::Vacant(entry) => {
                entry.insert(LRUKNode::new(i, self.current_ts));
                self.current_ts += 1;
            }
        }
    }

    pub fn pin(&mut self, i: FrameId) {
        if let Some(node) = self.nodes.get_mut(&i) {
            node.pin += 1;
        }
    }

    pub fn unpin(&mut self, i: FrameId) {
        if let Some(node) = self.nodes.get_mut(&i) {
            node.pin -= 1;
        }
    }

    pub fn remove(&mut self, i: FrameId) {
        match self.nodes.entry(i) {
            Entry::Occupied(node) => {
                let pins = node.get().pin;
                if pins != 0 {
                    eprintln!("WARN: frame {} is still pinned, {} pins", i, pins);
                }

                node.remove();
            }
            Entry::Vacant(_) => {}
        }
    }
}

pub struct LRU {
    inner: Mutex<LRUKReplacer>,
}

impl LRU {
    pub fn new(k: usize) -> Arc<Self> {
        Arc::new(Self { inner: Mutex::new(LRUKReplacer::new(k)) })
    }

    pub fn lock(&self) -> MutexGuard<'_, LRUKReplacer> {
        self.inner.lock().expect("todo")
    }

    pub fn evict(&self) -> Option<FrameId> {
        let mut replacer = self.inner.lock().expect("todo");
        replacer.evict()
    }

    pub fn record_access(&self, i: FrameId, a: AccessType) {
        let mut replacer = self.inner.lock().expect("todo");
        replacer.record_access(i, a)
    }

    pub fn pin(&self, i: FrameId) {
        let mut replacer = self.inner.lock().expect("todo");
        replacer.pin(i)
    }

    pub fn unpin(&self, i: FrameId) {
        let mut replacer = self.inner.lock().expect("todo");
        replacer.unpin(i)
    }

    pub fn remove(&self, i: FrameId) {
        let mut replacer = self.inner.lock().expect("todo");
        replacer.remove(i)
    }
}

#[cfg(test)]
mod test {
    use super::{AccessType, LRU};

    #[test]
    fn test_evict() {
        const K: usize = 2;
        let replacer = LRU::new(K);

        {
            for i in 0..8 {
                replacer.remove(i);
                replacer.record_access(i, AccessType::Get);
                replacer.pin(i);
            }

            for i in (0..8).rev() {
                replacer.unpin(i);

                let have = replacer.evict();
                let want = Some(i);
                assert!(want == have, "Want: {want:?}, Have: {have:?}");
            }
        }
    }
}
