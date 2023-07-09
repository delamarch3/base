use std::collections::{hash_map::Entry, HashMap};

struct Lruk {
    buf_i: usize,
    history: Vec<u64>,
    is_evictable: bool,
}

impl Lruk {
    pub fn new(buf_i: usize, ts: u64) -> Self {
        Self {
            buf_i,
            history: vec![ts],
            is_evictable: false,
        }
    }
}

#[derive(Default)]
pub struct LRUKReplacer {
    /// Maps index inside buffer pool to LRUK node
    nodes: HashMap<usize, Lruk>,
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
        let mut single_access: Vec<&Lruk> = Vec::new();
        for (id, node) in &self.nodes {
            if !node.is_evictable {
                continue;
            }

            let current = node.history.len();
            if current < self.k {
                single_access.push(node);
                continue;
            }

            let k = node.history.len() - self.k;
            let distance = node.history[current] - node.history[k];
            if distance > max.1 {
                max = (*id, distance);
            }
        }

        if max.1 != 0 {
            return Some(max.0);
        }

        if single_access.is_empty() {
            return None;
        }

        // If multiple frames have less than two recorded accesses, choose the one with the
        // earliest timestamp to evict
        let mut earliest: (usize, u64) = (0, u64::MAX);
        for node in &single_access {
            let ts = node.history[0];
            if ts < earliest.1 {
                earliest = (node.buf_i, ts);
            }
        }

        Some(earliest.0)
    }

    pub fn record_access(&mut self, buf_i: usize, _access_type: &AccessType) {
        match self.nodes.entry(buf_i) {
            Entry::Occupied(mut node) => {
                node.get_mut().history.push(self.current_ts);
                self.current_ts += 1;
            }
            Entry::Vacant(entry) => {
                entry.insert(Lruk::new(buf_i, self.current_ts));
                self.current_ts += 1;
            }
        }
    }

    pub fn set_evictable(&mut self, buf_i: usize, evictable: bool) {
        if let Some(node) = self.nodes.get_mut(&buf_i) {
            node.is_evictable = evictable;
        }
    }

    pub fn remove(&mut self, buf_i: usize) {
        match self.nodes.entry(buf_i) {
            Entry::Occupied(node) => {
                assert!(node.get().is_evictable);
                node.remove();
            }
            Entry::Vacant(_) => {
                eprintln!(
                    "ERROR: Attempt to remove frame that has not been registered in the replacer \
                    {buf_i}"
                );
            }
        }
    }
}
