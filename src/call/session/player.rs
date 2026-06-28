//! A paced audio **player** that drives a bridge player tap (hold music, IVR
//! prompts, ringback).
//!
//! Given a PCM buffer and a codec, it encodes 20 ms frames and pushes them into
//! the player tap's sink at real time (one frame every 20 ms). Pacing here is a
//! legitimate media clock — a source producing audio at the playout rate — not
//! polling. It can play once (a prompt) or loop (hold music), and stops on drop.
//!
//! Obtain the sink from [`MixerBridgeHandle::add_player`](super::mixer_bridge);
//! when the player finishes, remove the tap with `remove_leg`.

use std::time::Duration;

use audio_codec::{CodecType, PcmBuf, create_encoder, resample};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// A running player; cancels its task on drop.
pub struct Player {
    cancel: CancellationToken,
    _task: JoinHandle<()>,
}

impl Player {
    /// Start playing `pcm` (mono, at `source_rate`) as `codec`, pushing encoded
    /// frames into `sink` every 20 ms. `looping` repeats forever (hold music);
    /// otherwise it plays once and stops.
    pub fn start(
        pcm: PcmBuf,
        source_rate: u32,
        codec: CodecType,
        sink: mpsc::Sender<Vec<u8>>,
        looping: bool,
    ) -> Self {
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(
            pcm,
            source_rate,
            codec,
            sink,
            looping,
            cancel.clone(),
            None,
        ));
        Self {
            cancel,
            _task: task,
        }
    }

    /// Play `pcm` once, returning the player and a receiver that fires when the
    /// audio finishes (used for play-then-hangup on queue failure). Dropping the
    /// player still cancels playback.
    pub fn play_once(
        pcm: PcmBuf,
        source_rate: u32,
        codec: CodecType,
        sink: mpsc::Sender<Vec<u8>>,
    ) -> (Self, oneshot::Receiver<()>) {
        let cancel = CancellationToken::new();
        let (done_tx, done_rx) = oneshot::channel();
        let task = tokio::spawn(run(
            pcm,
            source_rate,
            codec,
            sink,
            false,
            cancel.clone(),
            Some(done_tx),
        ));
        (
            Self {
                cancel,
                _task: task,
            },
            done_rx,
        )
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

impl Drop for Player {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

async fn run(
    pcm: PcmBuf,
    source_rate: u32,
    codec: CodecType,
    sink: mpsc::Sender<Vec<u8>>,
    looping: bool,
    cancel: CancellationToken,
    mut done: Option<oneshot::Sender<()>>,
) {
    let rate = codec.samplerate();
    let pcm = if source_rate != rate {
        resample(&pcm, source_rate, rate)
    } else {
        pcm
    };
    if pcm.is_empty() {
        if let Some(done) = done {
            let _ = done.send(());
        }
        return;
    }
    let frame = (rate / 50).max(1) as usize;
    let mut enc = create_encoder(codec);
    let mut clock = tokio::time::interval(Duration::from_millis(20));
    clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pos = 0usize;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = clock.tick() => {
                if pos >= pcm.len() {
                    if looping {
                        pos = 0;
                    } else {
                        if let Some(done) = done.take() {
                            let _ = done.send(());
                        }
                        break;
                    }
                }
                let end = (pos + frame).min(pcm.len());
                let mut chunk = pcm[pos..end].to_vec();
                chunk.resize(frame, 0); // pad the final short frame with silence
                pos = end;
                let payload = enc.encode(&chunk);
                if sink.send(payload).await.is_err() {
                    break; // bridge gone
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::call::session::mixer_bridge::MixerBridge;
    use crate::call::session::rtp_socket::loopback;

    #[tokio::test]
    async fn player_drives_hold_audio_to_a_listener() {
        // A lone caller on the bridge.
        let (io_caller, _c_in, mut c_out) = loopback();
        let mut bridge = MixerBridge::new(8000);
        bridge.add_leg(CodecType::PCMU, io_caller);
        let handle = bridge.spawn();

        // Add a player tap and drive a 1 s constant tone (looping) into it.
        let (_tap, sink) = handle.add_player(CodecType::PCMU).await.expect("add player");
        let tone: PcmBuf = vec![3000i16; 8000];
        let _player = Player::start(tone, 8000, CodecType::PCMU, sink, true);

        // The caller should receive non-silent audio (the played tone).
        let mut best = 0.0f64;
        for _ in 0..20 {
            if let Ok(Some(p)) =
                tokio::time::timeout(Duration::from_millis(100), c_out.recv()).await
            {
                let mut dec = audio_codec::create_decoder(CodecType::PCMU);
                let pcm = dec.decode(&p);
                let mean = pcm.iter().map(|&s| (s as f64).abs()).sum::<f64>()
                    / pcm.len().max(1) as f64;
                if mean > best {
                    best = mean;
                }
                if best > 2000.0 {
                    break;
                }
            }
        }
        assert!(best > 2000.0, "listener should hear the played tone, peak {best}");
    }
}
