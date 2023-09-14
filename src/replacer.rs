use std::{
    collections::{hash_map::Entry, HashMap},
    time::Duration,
};

use crate::page_cache::FrameId;

struct LRUKNode {
    i: FrameId,
    history: Vec<u64>,
    pin: u64,
}

impl LRUKNode {
    pub fn new(i: usize, ts: u64) -> Self {
        Self {
            i,
            history: vec![ts],
            pin: 0,
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
        Self {
            k,
            ..Default::default()
        }
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
                assert!(node.get().pin == 0);
                node.remove();
            }
            Entry::Vacant(_) => {
                eprintln!(
                    "warn: attempt to remove frame that has not been registered in the replacer: \
                    {i}"
                );
            }
        }
    }
}

use std::sync::mpsc;
use tokio::sync::oneshot;

pub enum LRUKMessage {
    // Evict { reply: Oneshot<Option<FrameId>> },
    Evict {
        reply: oneshot::Sender<Option<FrameId>>,
    },
    RecordAccess(FrameId, AccessType),
    Pin(FrameId),
    Unpin(FrameId),
    Remove(FrameId),
}

pub struct LRUKActor {
    inner: LRUKReplacer,
    rx: mpsc::Receiver<LRUKMessage>,
}

impl LRUKActor {
    pub fn new(k: usize, rx: mpsc::Receiver<LRUKMessage>) -> Self {
        let inner = LRUKReplacer::new(k);

        Self { inner, rx }
    }

    pub fn run(&mut self) {
        while let Ok(m) = self.rx.recv() {
            match m {
                LRUKMessage::Evict { reply } => {
                    let ret = self.inner.evict();

                    if let Err(_) = reply.send(ret) {
                        eprintln!("replacer channel error: could not reply to evict message");
                    }
                }
                LRUKMessage::RecordAccess(i, a) => self.inner.record_access(i, a),
                LRUKMessage::Pin(i) => self.inner.pin(i),
                LRUKMessage::Unpin(i) => self.inner.unpin(i),
                LRUKMessage::Remove(i) => self.inner.remove(i),
            }
        }
    }
}

#[derive(Clone)]
pub struct LRUKHandle {
    tx: mpsc::Sender<LRUKMessage>,
}

impl LRUKHandle {
    pub fn new(k: usize) -> Self {
        let (tx, rx) = mpsc::channel();

        let mut replacer = LRUKActor::new(k, rx);
        let _jh = tokio::spawn(async move { replacer.run() });

        Self { tx }
    }

    pub async fn evict(&self) -> Option<FrameId> {
        let (tx, rx) = oneshot::channel();

        if let Err(e) = self.tx.send(LRUKMessage::Evict { reply: tx }) {
            eprintln!("replacer channel error: {e}");
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        match rx.await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("replacer channel error: {e}");
                None
            }
        }
    }

    pub fn record_access(&self, i: FrameId, a: AccessType) {
        if let Err(e) = self.tx.send(LRUKMessage::RecordAccess(i, a)) {
            eprintln!("replacer channel error: {e}");
        }
    }

    pub fn pin(&self, i: FrameId) {
        if let Err(e) = self.tx.send(LRUKMessage::Pin(i)) {
            eprintln!("replacer channel error: {e}");
        }
    }

    pub fn unpin(&self, i: FrameId) {
        if let Err(e) = self.tx.send(LRUKMessage::Unpin(i)) {
            eprintln!("replacer channel error: {e}");
        }
    }

    pub fn remove(&self, i: FrameId) {
        if let Err(e) = self.tx.send(LRUKMessage::Remove(i)) {
            eprintln!("replacer channel error: {e}");
        }
    }
}
