use crate::table_page::RelationID;

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct Pair2<A, B> {
    pub a: A,
    pub b: B,
}

impl<A, B> Pair2<A, B> {
    pub fn new(a: A, b: B) -> Self {
        Self { a, b }
    }
}

impl<A, B> PartialEq<(A, B)> for Pair2<A, B>
where
    A: PartialEq,
    B: PartialEq,
{
    fn eq(&self, other: &(A, B)) -> bool {
        self.a == other.0 && self.b == other.1
    }
}

impl<K> PartialOrd for Pair2<K, RelationID>
where
    K: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl<K> Ord for Pair2<K, RelationID>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.a.cmp(&other.a)
    }
}
