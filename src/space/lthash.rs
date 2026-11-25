//! Incremental set hashing for atproto space records.

use sha2::{Digest, Sha256};

pub const LANES: usize = 1024;
pub const LTHASH_STATE_BYTES: usize = LANES * 2;

/// A homomorphic hash of record elements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LtHash {
    lanes: [u16; LANES],
}

impl Default for LtHash {
    fn default() -> Self {
        Self::new()
    }
}

impl LtHash {
    pub fn new() -> Self {
        Self { lanes: [0; LANES] }
    }

    /// Import a serialized state. The exact 2,048-byte length is required.
    pub fn from_state(state: &[u8]) -> Result<Self, String> {
        if state.len() != LTHASH_STATE_BYTES {
            return Err(format!(
                "LtHash state must be {} bytes, got {}",
                LTHASH_STATE_BYTES,
                state.len()
            ));
        }

        let mut lanes = [0u16; LANES];
        for (lane, bytes) in lanes.iter_mut().zip(state.chunks_exact(2)) {
            *lane = u16::from_le_bytes([bytes[0], bytes[1]]);
        }
        Ok(Self { lanes })
    }

    /// Alias for `from_state`, useful at call sites that persist LtHash state.
    pub fn import(state: &[u8]) -> Result<Self, String> {
        Self::from_state(state)
    }

    pub fn add(&mut self, element: &str) -> &mut Self {
        let expanded = expand(element);
        for (lane, expanded_lane) in self.lanes.iter_mut().zip(expanded) {
            *lane = lane.wrapping_add(expanded_lane);
        }
        self
    }

    pub fn remove(&mut self, element: &str) -> &mut Self {
        let expanded = expand(element);
        for (lane, expanded_lane) in self.lanes.iter_mut().zip(expanded) {
            *lane = lane.wrapping_sub(expanded_lane);
        }
        self
    }

    /// Export the full serialized state.
    pub fn state(&self) -> [u8; LTHASH_STATE_BYTES] {
        let mut state = [0u8; LTHASH_STATE_BYTES];
        for (lane, bytes) in self.lanes.iter().zip(state.chunks_exact_mut(2)) {
            bytes.copy_from_slice(&lane.to_le_bytes());
        }
        state
    }

    pub fn export(&self) -> [u8; LTHASH_STATE_BYTES] {
        self.state()
    }

    pub fn digest(&self) -> [u8; 32] {
        Sha256::digest(self.state()).into()
    }

    pub fn is_empty(&self) -> bool {
        self.lanes.iter().all(|lane| *lane == 0)
    }

    pub fn equals(&self, other: &LtHash) -> bool {
        self == other
    }
}

fn expand(element: &str) -> [u16; LANES] {
    let mut expanded = [0u8; LTHASH_STATE_BYTES];
    blake3::Hasher::new()
        .update(element.as_bytes())
        .finalize_xof()
        .fill(&mut expanded);

    let mut lanes = [0u16; LANES];
    for (lane, bytes) in lanes.iter_mut().zip(expanded.chunks_exact(2)) {
        *lane = u16::from_le_bytes([bytes[0], bytes[1]]);
    }
    lanes
}

/// Format the canonical record element for LtHash.
pub fn format_record_element(collection: &str, rkey: &str, record_cid: &str) -> String {
    format!("{collection}/{rkey}/{record_cid}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_digest(hash: &LtHash) -> String {
        hash.digest()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    #[test]
    fn empty_state_and_digest_match_upstream() {
        let hash = LtHash::new();
        assert!(hash.is_empty());
        assert_eq!(hash.state(), [0u8; LTHASH_STATE_BYTES]);
        assert_eq!(
            hex_digest(&hash),
            "e5a00aa9991ac8a5ee3109844d84a55583bd20572ad3ffcd42792f3c36b183ad"
        );
    }

    #[test]
    fn upstream_snapshot_matches() {
        let mut hash = LtHash::new();
        hash.add("one").add("two");
        assert_eq!(
            hex_digest(&hash),
            "ae05cb6d224379d9710c290c8529945c5b0e0fde9ead30b9699057ce701c63e7"
        );
    }

    #[test]
    fn insertion_order_is_independent() {
        let mut ab = LtHash::new();
        ab.add("a").add("b");
        let mut ba = LtHash::new();
        ba.add("b").add("a");
        assert_eq!(ab, ba);
        assert_eq!(ab.digest(), ba.digest());
    }

    #[test]
    fn add_remove_cancellation_and_updates() {
        let mut hash = LtHash::new();
        hash.add("a");
        hash.remove("a");
        assert!(hash.is_empty());

        let mut updated = LtHash::new();
        updated
            .remove("collection/rkey/bafyold")
            .add("collection/rkey/bafynew");

        let mut expected = LtHash::new();
        expected
            .add("collection/rkey/bafynew")
            .remove("collection/rkey/bafyold");
        assert_eq!(updated, expected);
    }

    #[test]
    fn state_round_trips_and_rejects_wrong_lengths() {
        let mut hash = LtHash::new();
        hash.add("a").add("b");

        let restored = LtHash::from_state(&hash.state()).unwrap();
        assert_eq!(hash, restored);
        assert!(LtHash::from_state(&[0u8; 32]).is_err());
    }

    #[test]
    fn record_element_uses_canonical_fields() {
        assert_eq!(
            format_record_element("app.bsky.feed.post", "3abc", "bafy-record"),
            "app.bsky.feed.post/3abc/bafy-record"
        );
    }

    #[test]
    fn state_export_does_not_alias_internal_state() {
        let mut hash = LtHash::new();
        hash.add("a");
        let mut exported = hash.state();
        exported[0] ^= 0xff;
        assert_ne!(exported, hash.state());
    }
}
