//! Borrowed, allocation-free view of an EIP-2718 transaction exactly as it sits in an L2
//! feed message.
//!
//! The feed carries signed transactions as raw RLP. A full `TxEnvelope::decode_2718_exact`
//! copies the calldata into a fresh `Bytes`, builds the access list / authorization list
//! `Vec`s and validates the signature fields; that is ~350 ns plus heap traffic per
//! transaction, and nearly every transaction in a block is irrelevant. This view walks the
//! RLP headers in place and hands out `&[u8]` slices of the original buffer.
//!
//! It is only ever a *prefilter*: a transaction it classifies as irrelevant is dropped, one it
//! cannot parse (or finds relevant) goes through the full decoder, which stays the single
//! source of truth for validity and for the sender recovery.

use alloy_rlp::Header;

/// Fields the trigger logic needs, borrowed from the wire bytes.
#[derive(Clone, Copy, Debug)]
pub struct RawTxView<'a> {
    /// 0 for a legacy transaction, otherwise the EIP-2718 type byte.
    pub tx_type: u8,
    /// `None` for contract creation.
    pub to: Option<&'a [u8; 20]>,
    /// Big-endian wei amount as RLP encodes it (no leading zeros; empty = 0).
    pub value_be: &'a [u8],
    pub input: &'a [u8],
}

impl<'a> RawTxView<'a> {
    /// Locate `to`, `value` and `input` without copying. `None` means "could not tell":
    /// callers must then fall back to the full decoder, never treat it as a negative.
    #[inline]
    pub fn parse(raw: &'a [u8]) -> Option<Self> {
        // Number of RLP items that precede `to` in each transaction type:
        //   legacy:  nonce, gasPrice, gasLimit
        //   0x01:    chainId, nonce, gasPrice, gasLimit
        //   0x02-04: chainId, nonce, maxPriorityFee, maxFee, gasLimit
        let (tx_type, mut buf, skip) = match *raw.first()? {
            0x01 => (1, &raw[1..], 4),
            t @ 0x02..=0x04 => (t, &raw[1..], 5),
            0xc0..=0xff => (0, raw, 3),
            _ => return None,
        };
        let mut fields = Header::decode_bytes(&mut buf, true).ok()?;
        for _ in 0..skip {
            Header::decode_bytes(&mut fields, false).ok()?;
        }
        let to = Header::decode_bytes(&mut fields, false).ok()?;
        let to = match to.len() {
            0 => None,
            20 => Some(<&[u8; 20]>::try_from(to).ok()?),
            _ => return None,
        };
        let value_be = Header::decode_bytes(&mut fields, false).ok()?;
        let input = Header::decode_bytes(&mut fields, false).ok()?;
        Some(Self { tx_type, to, value_be, input })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_garbage_without_panicking() {
        for raw in [&[][..], &[0x00], &[0x05, 0xc0], &[0x02], &[0x02, 0xc1], &[0xc1, 0x01], &[0xff; 9]] {
            assert!(RawTxView::parse(raw).is_none(), "{raw:?}");
        }
    }

    #[test]
    fn truncated_lists_are_unparsed_not_misread() {
        // A 1559 list that claims 200 bytes but carries 3.
        assert!(RawTxView::parse(&[0x02, 0xf8, 0xc8, 0x01, 0x02, 0x03]).is_none());
    }
}
