///! # Bloom Filter
///!
///! A space-efficient probabilistic data structure for set membership testing.
///!
///! A bloom filter can tell you with certainty that a key is **not** in the set
///! (no false negatives), but may occasionally report that a key **is** in the
///! set when it isn't (false positives). The false positive rate depends on the
///! number of bits and hash functions used.
///!
///! ## Usage in RiptideKV
///!
///! Each SSTable embeds a bloom filter built from its keys. During point lookups
///! the engine checks the bloom filter first -- if it says "not present", the
///! SSTable is skipped entirely, avoiding expensive index lookups and disk I/O.
///!
///! ## Example
///!
///! ```rust,no_run
///! use bloom::BloomFilter;
///!
///! let mut bf = BloomFilter::new(1000, 0.01);
///! bf.insert(b"hello");
///! assert!(bf.may_contain(b"hello"));
///! ```
use std::io::{self, Read, Write};

/// A bloom filter backed by a bit vector with `k` independent hash functions.
///
/// Uses double hashing: `h(i) = h1 + i * h2` where `h1` and `h2` are derived
/// from FNV-1a with two different seeds.
pub struct BloomFilter {
    /// The bit vector storing the filter state.
    bits: Vec<u8>,
    /// Number of bits in the filter.
    num_bits: u64,
    /// Number of hash functions (k).
    num_hashes: u32,
}

impl BloomFilter {
    /// Creates a new bloom filter sized for `expected_items` with the given
    /// target `false_positive_rate`.
    ///
    /// # Panics
    ///
    /// Panics if `expected_items` is 0 or `false_positive_rate` is not in `(0, 1)`.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        assert!(expected_items > 0, "expected_items must be > 0");
        assert!(
            false_positive_rate > 0.0 && false_positive_rate < 1.0,
            "false_positive_rate must be in (0, 1)"
        );

        // Optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let n = expected_items as f64;
        let m = (-n * false_positive_rate.ln() / (std::f64::consts::LN_2.powi(2))).ceil() as u64;
        let m = m.max(8);

        // Optimal number of hashes: k = (m/n) * ln(2)
        let k = ((m as f64 / n) * std::f64::consts::LN_2).ceil() as u32;
        let k = k.max(1);

        let byte_len = ((m + 7) / 8) as usize;

        Self {
            bits: vec![0u8; byte_len],
            num_bits: m,
            num_hashes: k,
        }
    }

    /// Creates a bloom filter from raw parts (used during deserialization).
    fn from_raw(bits: Vec<u8>, num_bits: u64, num_hashes: u32) -> Self {
        Self {
            bits,
            num_bits,
            num_hashes,
        }
    }

    /// Inserts a key into the bloom filter.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(h1, h2, i);
            self.set_bit(bit_idx);
        }
    }

    /// Returns `true` if the key **might** be in the set, `false` if it is
    /// **definitely not** in the set.
    #[must_use]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(h1, h2, i);
            if !self.get_bit(bit_idx) {
                return false;
            }
        }
        true
    }

    /// Returns the number of bits in the filter.
    #[must_use]
    pub fn num_bits(&self) -> u64 {
        self.num_bits
    }

    /// Returns the number of hash functions.
    #[must_use]
    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    /// Returns the size of the serialized bloom filter in bytes.
    ///
    /// Layout: `num_bits(u64) + num_hashes(u32) + bits_len(u32) + bits`.
    #[must_use]
    pub fn serialized_size(&self) -> usize {
        8 + 4 + 4 + self.bits.len()
    }

    /// Serializes the bloom filter to a writer.
    ///
    /// Wire format (all little-endian):
    /// ```text
    /// [num_bits: u64][num_hashes: u32][bits_len: u32][bits: bytes]
    /// ```
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.num_bits.to_le_bytes())?;
        w.write_all(&self.num_hashes.to_le_bytes())?;
        w.write_all(&(self.bits.len() as u32).to_le_bytes())?;
        w.write_all(&self.bits)?;
        Ok(())
    }

    /// Deserializes a bloom filter from a reader.
    pub fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut buf8 = [0u8; 8];
        let mut buf4 = [0u8; 4];

        r.read_exact(&mut buf8)?;
        let num_bits = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf4)?;
        let num_hashes = u32::from_le_bytes(buf4);

        r.read_exact(&mut buf4)?;
        let bits_len = u32::from_le_bytes(buf4) as usize;

        // Safety cap: bloom filter should not exceed 128 MiB
        const MAX_BLOOM_BYTES: usize = 128 * 1024 * 1024;
        if bits_len > MAX_BLOOM_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bloom filter too large: {} bytes", bits_len),
            ));
        }

        let mut bits = vec![0u8; bits_len];
        r.read_exact(&mut bits)?;

        Ok(Self::from_raw(bits, num_bits, num_hashes))
    }

    // ---- Internal helpers ----

    /// Computes two independent 64-bit hashes using FNV-1a with different seeds.
    fn hash_pair(&self, key: &[u8]) -> (u64, u64) {
        let h1 = fnv1a_64(key, 0xcbf29ce484222325);
        let h2 = fnv1a_64(key, 0x517cc1b727220a95);
        (h1, h2)
    }

    /// Double hashing: h(i) = (h1 + i * h2) mod num_bits.
    fn get_bit_index(&self, h1: u64, h2: u64, i: u32) -> u64 {
        h1.wrapping_add((i as u64).wrapping_mul(h2)) % self.num_bits
    }

    fn set_bit(&mut self, idx: u64) {
        let byte_idx = (idx / 8) as usize;
        let bit_offset = (idx % 8) as u8;
        self.bits[byte_idx] |= 1 << bit_offset;
    }

    fn get_bit(&self, idx: u64) -> bool {
        let byte_idx = (idx / 8) as usize;
        let bit_offset = (idx % 8) as u8;
        (self.bits[byte_idx] >> bit_offset) & 1 == 1
    }
}

impl std::fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilter")
            .field("num_bits", &self.num_bits)
            .field("num_hashes", &self.num_hashes)
            .field("bytes", &self.bits.len())
            .finish()
    }
}

/// FNV-1a 64-bit hash with a configurable starting basis.
fn fnv1a_64(data: &[u8], basis: u64) -> u64 {
    const FNV_PRIME: u64 = 0x00000100000001b3;
    let mut hash = basis;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests;
