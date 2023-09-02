use std::collections::{hash_map::Entry, HashMap};

struct LRUKNode {
    i: usize,
    history: Vec<u64>,
    is_evictable: bool,
}

impl LRUKNode {
    pub fn new(i: usize, ts: u64) -> Self {
        Self {
            i,
            history: vec![ts],
            is_evictable: false,
        }
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

#[derive(Default)]
pub struct LRUKReplacer {
    /// Maps index inside page manager to LRUK node
    nodes: HashMap<usize, LRUKNode>,
    current_ts: u64,
    k: usize,
}

pub enum AccessType {
    Get,
    Scan,
}

impl LRUKReplacer {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            ..Default::default()
        }
    }

    pub fn evict(&mut self) -> Option<usize> {
        let mut max: (usize, u64) = (0, 0);
        let mut single_access: Vec<&LRUKNode> = Vec::new();
        for (id, node) in &self.nodes {
            if !node.is_evictable {
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

    pub fn record_access(&mut self, i: usize, _access_type: &AccessType) {
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

    pub fn set_evictable(&mut self, i: usize, evictable: bool) {
        if let Some(node) = self.nodes.get_mut(&i) {
            node.is_evictable = evictable;
        }
    }

    pub fn remove(&mut self, i: usize) {
        match self.nodes.entry(i) {
            Entry::Occupied(node) => {
                assert!(node.get().is_evictable);
                node.remove();
            }
            Entry::Vacant(_) => {
                eprintln!(
                    "ERROR: Attempt to remove frame that has not been registered in the replacer \
                    {i}"
                );
            }
        }
    }
}
