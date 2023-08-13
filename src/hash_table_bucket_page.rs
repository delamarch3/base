// No hashing in the bucket, key/value pairs are inserted/fetched by scanning

pub struct Bucket {
    occupied: [u8; 512],
    readable: [u8; 512],
}
