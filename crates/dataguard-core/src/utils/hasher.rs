use std::hash::{BuildHasher, Hasher};
use xxhash_rust::xxh3::Xxh3;

#[derive(Default, Clone)]
pub struct Xxh3Hasher(Xxh3);

impl Hasher for Xxh3Hasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0.finish()
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
}

#[derive(Clone, Default)]
pub struct Xxh3Builder;

impl BuildHasher for Xxh3Builder {
    type Hasher = Xxh3Hasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        Xxh3Hasher(Xxh3::new())
    }
}
