//! A per-tap **packet** FIFO (jitter buffer).
//!
//! Inbound RTP arrives irregularly (jitter, bursts, gaps). The FIFO holds the
//! codec payloads themselves — *not* decoded PCM — so the passthrough case stays
//! cheap: a destination on the same codec pulls a packet and forwards it
//! untouched (no decode, no re-encode). Only when a destination must mix or
//! transcode is a pulled packet decoded.
//!
//! It is always on for every tap. Two safety rules keep it honest:
//!
//! * **Underrun → `None`.** Pulling from an empty FIFO yields nothing; the mixer
//!   treats a missing packet as silence so a lagging participant never stalls
//!   the room.
//! * **Overflow → drop oldest.** Capped depth bounds latency; a fast/buggy
//!   sender can't grow the backlog without bound.
//!
//! No packet-duration is assumed: payloads may carry any frame length (Opus
//! 10/20/40/60 ms, variable RTP, or non-RTP transports). The FIFO is a pure
//! queue; the mix path aligns sources by *decoded sample count*, decoding a
//! pulled packet into a per-tap PCM remainder and consuming a fixed frame of
//! samples from there. Passthrough never decodes, so duration is irrelevant to
//! it.

use std::collections::VecDeque;

/// A bounded FIFO of codec payloads (one tap's inbound packet stream).
pub struct PacketFifo {
    buf: VecDeque<Vec<u8>>,
    max_packets: usize,
}

impl PacketFifo {
    /// Create a FIFO holding at most `max_packets` (the latency cap). Depth is
    /// in packets, independent of each packet's audio duration.
    pub fn new(max_packets: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(max_packets.max(1)),
            max_packets: max_packets.max(1),
        }
    }

    /// Push an inbound payload. If this would exceed the cap, the oldest packet
    /// is dropped to keep the buffer current (bounded latency).
    pub fn push(&mut self, payload: Vec<u8>) {
        self.buf.push_back(payload);
        while self.buf.len() > self.max_packets {
            self.buf.pop_front();
        }
    }

    /// Pull the next payload, or `None` on underrun (treated as silence).
    pub fn pull(&mut self) -> Option<Vec<u8>> {
        self.buf.pop_front()
    }

    /// Number of buffered packets.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_then_pull_is_fifo_order() {
        let mut f = PacketFifo::new(50);
        f.push(vec![1, 2, 3]);
        f.push(vec![4, 5, 6]);
        assert_eq!(f.len(), 2);
        assert_eq!(f.pull(), Some(vec![1, 2, 3]));
        assert_eq!(f.pull(), Some(vec![4, 5, 6]));
        assert_eq!(f.pull(), None);
        assert!(f.is_empty());
    }

    #[test]
    fn underrun_returns_none() {
        let mut f = PacketFifo::new(50);
        assert_eq!(f.pull(), None);
    }

    #[test]
    fn overflow_drops_oldest() {
        let mut f = PacketFifo::new(2);
        f.push(vec![1]);
        f.push(vec![2]);
        f.push(vec![3]); // drops [1]
        assert_eq!(f.len(), 2);
        assert_eq!(f.pull(), Some(vec![2]));
        assert_eq!(f.pull(), Some(vec![3]));
    }

    #[test]
    fn passthrough_payload_survives_unmodified() {
        let mut f = PacketFifo::new(50);
        let payload: Vec<u8> = (0..160).map(|i| i as u8).collect();
        f.push(payload.clone());
        // The exact bytes come back out — the basis for codec passthrough.
        assert_eq!(f.pull(), Some(payload));
    }
}
