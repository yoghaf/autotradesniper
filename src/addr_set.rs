//! Allocation-free membership test over raw 20-byte addresses.
//!
//! The hot path asks "is this sender / recipient one we care about?" for every candidate
//! transaction in every block. `HashSet<Address>` answers that with a SipHash over the key
//! plus, for the shared watchlist, an async `tokio::RwLock` read. Here the answer is one
//! multiply, one 20-byte compare in the common case, and no lock at all on the read side
//! (callers keep the set in a snapshot they swap on the rare write).

use alloy::primitives::Address;

const ZERO: [u8; 20] = [0; 20];

/// Open-addressing set of `[u8; 20]` keys with linear probing and a load factor <= 0.5, so a
/// probe always terminates at an empty slot. Addresses are keccak-derived (uniform), which is
/// why a cheap fold of their bytes is a good-enough hash; a vanity address can at worst make
/// one probe chain longer, never produce a false answer.
#[derive(Clone, Debug, Default)]
pub struct AddressSet {
    slots: Box<[[u8; 20]]>,
    mask: usize,
    len: usize,
    /// The all-zero address doubles as the "empty slot" marker, so it is tracked out of band.
    has_zero: bool,
}

#[inline(always)]
fn fold(a: &[u8; 20]) -> u64 {
    let a0 = u64::from_le_bytes(a[0..8].try_into().unwrap());
    let a1 = u64::from_le_bytes(a[8..16].try_into().unwrap());
    let a2 = u32::from_le_bytes(a[16..20].try_into().unwrap()) as u64;
    (a0 ^ a1.rotate_left(21) ^ a2.rotate_left(42)).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

impl AddressSet {
    pub fn new<'a>(addrs: impl IntoIterator<Item = &'a Address>) -> Self {
        let keys: Vec<[u8; 20]> = addrs.into_iter().map(|a| a.0 .0).collect();
        let mut set = Self::default();
        if keys.is_empty() {
            return set;
        }
        let cap = (keys.len() * 2).next_power_of_two().max(4);
        set.slots = vec![ZERO; cap].into_boxed_slice();
        set.mask = cap - 1;
        for key in keys {
            set.insert(key);
        }
        set
    }

    fn insert(&mut self, key: [u8; 20]) {
        if key == ZERO {
            if !self.has_zero {
                self.has_zero = true;
                self.len += 1;
            }
            return;
        }
        let mut i = (fold(&key) >> 32) as usize & self.mask;
        loop {
            if self.slots[i] == key {
                return;
            }
            if self.slots[i] == ZERO {
                self.slots[i] = key;
                self.len += 1;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Bitwise membership test on the raw key. No hashing of the input through a `Hasher`, no
    /// allocation, no panic path other than the (masked, hence in-range) slot index.
    #[inline(always)]
    pub fn contains_bytes(&self, key: &[u8; 20]) -> bool {
        if self.len == 0 {
            return false;
        }
        if *key == ZERO {
            return self.has_zero;
        }
        let mut i = (fold(key) >> 32) as usize & self.mask;
        loop {
            let slot = &self.slots[i];
            if slot == key {
                return true;
            }
            if *slot == ZERO {
                return false;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    pub fn contains(&self, addr: &Address) -> bool {
        self.contains_bytes(&addr.0 .0)
    }
}

impl<'a> FromIterator<&'a Address> for AddressSet {
    fn from_iter<T: IntoIterator<Item = &'a Address>>(iter: T) -> Self {
        Self::new(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn empty_set_matches_nothing() {
        let set = AddressSet::new(&[]);
        assert!(set.is_empty());
        assert!(!set.contains(&Address::ZERO));
        assert!(!set.contains(&Address::repeat_byte(1)));
    }

    #[test]
    fn zero_address_is_a_legal_member() {
        let set = AddressSet::new(&[Address::ZERO]);
        assert_eq!(set.len(), 1);
        assert!(set.contains(&Address::ZERO));
        assert!(!set.contains(&Address::repeat_byte(1)));
        let without = AddressSet::new(&[Address::repeat_byte(1)]);
        assert!(!without.contains(&Address::ZERO));
    }

    #[test]
    fn agrees_with_hashset_on_adversarial_keys() {
        // Keys that differ in a single byte, in every position, plus clustered prefixes: the
        // set must answer exactly like `HashSet` for members and near-misses alike.
        let mut members = HashSet::new();
        for pos in 0..20 {
            for b in [1u8, 0x80, 0xff] {
                let mut a = [0u8; 20];
                a[pos] = b;
                members.insert(Address::from(a));
            }
        }
        members.insert(Address::repeat_byte(0xaa));
        members.insert(Address::ZERO);
        let set = AddressSet::new(members.iter());
        assert_eq!(set.len(), members.len());
        for m in &members {
            assert!(set.contains(m));
        }
        for pos in 0..20 {
            for b in [2u8, 0x7f, 0xfe] {
                let mut a = [0u8; 20];
                a[pos] = b;
                assert!(!set.contains(&Address::from(a)), "false positive at byte {pos}");
            }
        }
        assert!(!set.contains(&Address::repeat_byte(0xab)));
    }

    #[test]
    fn duplicates_collapse() {
        let a = Address::repeat_byte(7);
        let set = AddressSet::new(&[a, a, a]);
        assert_eq!(set.len(), 1);
        assert!(set.contains(&a));
    }

    #[test]
    fn survives_many_members_without_losing_any() {
        let members: Vec<Address> = (0u32..5_000)
            .map(|i| {
                let mut a = [0u8; 20];
                a[16..20].copy_from_slice(&i.to_be_bytes());
                a[0] = (i % 7) as u8;
                Address::from(a)
            })
            .collect();
        let set = AddressSet::new(members.iter());
        assert_eq!(set.len(), members.len());
        assert!(members.iter().all(|m| set.contains(m)));
        assert!(!set.contains(&Address::repeat_byte(0xee)));
    }
}
