//! Unified media mixer: one engine for 2-party bridging *and* N-party
//! conferencing, where the per-participant **tap** decides whether to pass a
//! packet through untouched, transcode it, or mix it.
//!
//! # Two routing paths
//!
//! A destination's behaviour depends only on how many *sources* are routed to it
//! (mix-minus-self):
//!
//! | sources → a tap     | action                                          |
//! |---------------------|-------------------------------------------------|
//! | 1, same codec       | **passthrough** the payload (no decode/encode)  |
//! | 1, different codec  | **transcode** (decode → resample → encode)      |
//! | >1                  | **mix** (decode each → room PCM → sum → encode)  |
//!
//! The **single-source** case ([`UnifiedMixer::on_inbound`]) is event-driven and
//! packet-paced: a packet arriving on a leg is routed straight out, no timer,
//! and same-codec stays byte-for-byte passthrough. This is every plain 2-party
//! call.
//!
//! The **multi-source** case ([`UnifiedMixer::mix_frame`]) needs a media clock,
//! because independent sources must be aligned to a common playout instant. Each
//! tap has an always-on [`PacketFifo`]; the mix decodes pulled packets into a
//! per-tap PCM remainder and consumes a fixed *frame of samples*, so it makes no
//! assumption about packet duration (Opus 10/40/60 ms, variable RTP, non-RTP
//! transports all work). A recorder is a sink-only tap; a player is a
//! source-only tap.
//!
//! Audio-only (the `audio-codec` crate). Video later plugs the same tap model
//! into an ffmpeg-backed codec layer. Transport (RTP/SRTP/WebRTC) wraps this
//! core — it deals only in codec'd payloads, never sockets.

use std::collections::{HashMap, VecDeque};

use audio_codec::{CodecType, Decoder, Encoder, PcmBuf, create_decoder, create_encoder, resample};

use crate::media::audio_fifo::PacketFifo;

/// Identifies a tap (participant connection) within a mixer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TapId(pub u64);

/// What a tap is. Roles decide who is a source (contributes audio) and who is a
/// sink (receives audio); the switch/transport layer also uses them to bind a
/// tap (participant ↔ RTP socket, player ← file/TTS, recorder → file writer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TapRole {
    /// A live participant: caller, callee, conference member (source + sink).
    Participant,
    /// A prompt source: IVR/file playback, TTS, ringback, MOH (source only).
    Player,
    /// A sink that receives the room mix for recording (sink only).
    Recorder,
}

impl TapRole {
    /// Whether this tap contributes audio to others' mixes.
    fn is_source(self) -> bool {
        matches!(self, TapRole::Participant | TapRole::Player)
    }
    /// Whether this tap receives routed/mixed audio.
    fn is_sink(self) -> bool {
        matches!(self, TapRole::Participant | TapRole::Recorder)
    }
}

/// A codec'd audio payload (one packet's worth of samples), tagged with its
/// codec. No RTP headers — sequence/timestamp/SSRC is the transport layer's job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub codec: CodecType,
    pub payload: Vec<u8>,
}

/// Output frame cadence for the mix clock: 20 ms of audio at the room rate. This
/// is the *playout* granularity, not an assumption about inbound packet sizes.
fn frame_samples(room_rate: u32) -> usize {
    (room_rate / 50) as usize
}

struct Tap {
    codec: CodecType,
    role: TapRole,
    decoder: Box<dyn Decoder>,
    encoder: Box<dyn Encoder>,
    /// Always-on inbound jitter buffer (codec payloads, any duration).
    fifo: PacketFifo,
    /// Decoded PCM remainder at the room rate, carried between mix frames so a
    /// long packet (e.g. 60 ms Opus) spans several output frames correctly.
    stage: VecDeque<i16>,
}

impl Tap {
    fn rate(&self) -> u32 {
        self.codec.samplerate()
    }

    /// Decode buffered packets into the PCM stage until at least `frame` room-rate
    /// samples are available, then consume one frame (silence-padded on
    /// underrun). Duration-agnostic: input packets of any length are absorbed.
    fn pull_room_frame(&mut self, room_rate: u32, frame: usize) -> PcmBuf {
        while self.stage.len() < frame {
            let Some(payload) = self.fifo.pull() else {
                break;
            };
            let pcm = self.decoder.decode(&payload);
            let pcm = if self.rate() != room_rate {
                resample(&pcm, self.rate(), room_rate)
            } else {
                pcm
            };
            self.stage.extend(pcm);
        }
        let take = frame.min(self.stage.len());
        let mut out: PcmBuf = self.stage.drain(..take).collect();
        out.resize(frame, 0);
        out
    }
}

/// The unified mixer. Sources/sinks are full-mesh mix-minus-self.
pub struct UnifiedMixer {
    room_rate: u32,
    taps: HashMap<TapId, Tap>,
    next_id: u64,
    /// Per-(source, destination) linear gain override. Takes precedence over the
    /// source default. The whisper mechanism: audible to one sink only.
    gains: HashMap<(TapId, TapId), f32>,
    /// A source's gain into any sink without an explicit override (absent =
    /// 1.0). Setting 0.0 mutes the source into *every* sink, including taps
    /// added later — conference mute and listen-only supervisor.
    source_default: HashMap<TapId, f32>,
}

impl UnifiedMixer {
    /// `room_rate` is the PCM rate used for the N-party mix (e.g. 48000 for an
    /// Opus-native room, 8000 for telephony). It does not affect passthrough or
    /// single-source transcode.
    pub fn new(room_rate: u32) -> Self {
        Self {
            room_rate,
            taps: HashMap::new(),
            next_id: 1,
            gains: HashMap::new(),
            source_default: HashMap::new(),
        }
    }

    /// Set the linear gain of `src` into `dst` (1.0 = unchanged, 0.0 = muted).
    /// Negative values are clamped to 0.
    pub fn set_gain(&mut self, src: TapId, dst: TapId, gain: f32) {
        self.gains.insert((src, dst), gain.max(0.0));
    }

    /// The gain a source has into sinks without an explicit per-pair override
    /// (1.0 = normal, 0.0 = muted everywhere). The base for whisper (set 0, then
    /// override the one sink to 1).
    pub fn set_source_default(&mut self, src: TapId, gain: f32) {
        self.source_default.insert(src, gain.max(0.0));
    }

    /// Mute `src` into every sink (conference mute / listen-only supervisor),
    /// applying to taps added later too.
    pub fn mute_source(&mut self, src: TapId) {
        self.set_source_default(src, 0.0);
    }

    /// Restore `src` to audible: clears its default and any per-pair overrides.
    pub fn unmute_source(&mut self, src: TapId) {
        self.source_default.remove(&src);
        self.gains.retain(|&(s, _), _| s != src);
    }

    /// The effective gain of `src` into `dst`: an explicit per-pair override if
    /// set, else the source default, else 1.0.
    fn gain(&self, src: TapId, dst: TapId) -> f32 {
        if let Some(&g) = self.gains.get(&(src, dst)) {
            return g;
        }
        self.source_default.get(&src).copied().unwrap_or(1.0)
    }

    /// Add a live participant (source + sink) with the given codec.
    pub fn add_tap(&mut self, codec: CodecType) -> TapId {
        self.add_tap_with_role(codec, TapRole::Participant)
    }

    /// Add a prompt source (IVR/file/TTS/ringback/MOH) — source only.
    pub fn add_player(&mut self, codec: CodecType) -> TapId {
        self.add_tap_with_role(codec, TapRole::Player)
    }

    /// Add a sink-only recorder tap that receives the room mix in `codec`.
    pub fn add_recorder(&mut self, codec: CodecType) -> TapId {
        self.add_tap_with_role(codec, TapRole::Recorder)
    }

    /// Add a tap with an explicit role.
    pub fn add_tap_with_role(&mut self, codec: CodecType, role: TapRole) -> TapId {
        let id = TapId(self.next_id);
        self.next_id += 1;
        // ~1 s of jitter capacity, generous since depth is in packets.
        let fifo = PacketFifo::new(64);
        self.taps.insert(
            id,
            Tap {
                codec,
                role,
                decoder: create_decoder(codec),
                encoder: create_encoder(codec),
                fifo,
                stage: VecDeque::new(),
            },
        );
        id
    }

    /// The role of a tap, if present.
    pub fn role(&self, id: TapId) -> Option<TapRole> {
        self.taps.get(&id).map(|t| t.role)
    }

    pub fn remove_tap(&mut self, id: TapId) {
        self.taps.remove(&id);
        self.gains.retain(|&(s, d), _| s != id && d != id);
        self.source_default.remove(&id);
    }

    pub fn tap_count(&self) -> usize {
        self.taps.len()
    }

    /// Count of source-role taps (participants + players).
    fn source_count(&self) -> usize {
        self.taps.values().filter(|t| t.role.is_source()).count()
    }

    /// Whether any sink has more than one source — i.e. real mixing is required
    /// and the media clock ([`mix_frame`]) must run. False for a plain 2-party
    /// call, which the event-driven [`on_inbound`] path handles without a timer.
    pub fn needs_clock(&self) -> bool {
        let sources = self.source_count();
        self.taps.values().any(|t| {
            t.role.is_sink() && sources - if t.role.is_source() { 1 } else { 0 } > 1
        })
    }

    // -- Event-driven single-source path (no timer) --------------------------

    /// Route one inbound payload from `source` to every destination whose *only*
    /// source is `source`: passthrough if codecs match, transcode otherwise.
    /// Destinations with more than one source are left to [`mix_frame`] (they
    /// need the media clock). This is the no-timer fast path for 2-party calls.
    pub fn on_inbound(&mut self, source: TapId, payload: Vec<u8>) -> Vec<(TapId, Packet)> {
        if !self.taps.contains_key(&source) {
            return Vec::new();
        }
        let src_codec = self.taps[&source].codec;
        let src_rate = self.taps[&source].rate();
        let sources = self.source_count();

        // Destinations whose sole source is `source`. (When exactly one other
        // source exists, it is necessarily `source`.)
        let mut single: Vec<TapId> = Vec::new();
        for (&id, t) in &self.taps {
            if id == source || !t.role.is_sink() {
                continue;
            }
            let others = sources - if t.role.is_source() { 1 } else { 0 };
            if others == 1 {
                single.push(id);
            }
        }
        if single.is_empty() {
            return Vec::new();
        }

        // We must decode if any destination needs a different codec, or a
        // non-unity gain (which precludes byte passthrough).
        let need_pcm = single
            .iter()
            .any(|&d| self.taps[&d].codec != src_codec || self.gain(source, d) != 1.0);
        let src_pcm = if need_pcm {
            Some(self.taps.get_mut(&source).unwrap().decoder.decode(&payload))
        } else {
            None
        };

        let mut out = Vec::with_capacity(single.len());
        for dst in single {
            let g = self.gain(source, dst);
            if g == 0.0 {
                continue; // muted into this sink
            }
            let dst_codec = self.taps[&dst].codec;
            let dst_rate = self.taps[&dst].rate();
            let payload_out = if dst_codec == src_codec && g == 1.0 {
                payload.clone() // passthrough
            } else {
                let pcm = src_pcm.as_ref().unwrap();
                let mut pcm = if src_rate != dst_rate {
                    resample(pcm, src_rate, dst_rate)
                } else {
                    pcm.clone()
                };
                if g != 1.0 {
                    for s in pcm.iter_mut() {
                        *s = (*s as f32 * g) as i16;
                    }
                }
                self.taps.get_mut(&dst).unwrap().encoder.encode(&pcm)
            };
            out.push((
                dst,
                Packet {
                    codec: dst_codec,
                    payload: payload_out,
                },
            ));
        }
        out
    }

    // -- Clock-driven mixing path (FIFO + media clock) -----------------------

    /// Buffer an inbound payload into `source`'s jitter FIFO for the mix path.
    pub fn feed(&mut self, source: TapId, payload: Vec<u8>) {
        if let Some(tap) = self.taps.get_mut(&source)
            && tap.role.is_source()
        {
            tap.fifo.push(payload);
        }
    }

    /// Produce one mixed output frame for every sink, pulling an aligned frame
    /// from each source's FIFO (silence on underrun). Called on the media clock
    /// whenever a destination has more than one source. Returns one packet per
    /// sink.
    pub fn mix_frame(&mut self) -> HashMap<TapId, Packet> {
        let room_rate = self.room_rate;
        let frame = frame_samples(room_rate);

        // Pull one room-rate frame from each source tap.
        let mut source_pcm: HashMap<TapId, PcmBuf> = HashMap::new();
        for (&id, tap) in self.taps.iter_mut() {
            if tap.role.is_source() {
                source_pcm.insert(id, tap.pull_room_frame(room_rate, frame));
            }
        }

        let sinks: Vec<TapId> = self
            .taps
            .iter()
            .filter(|(_, t)| t.role.is_sink())
            .map(|(id, _)| *id)
            .collect();

        let mut out = HashMap::new();
        for dst in sinks {
            // Weighted mix-minus-self: each source contributes at gain(src, dst).
            let mut acc = vec![0i32; frame];
            for (&src, pcm) in source_pcm.iter() {
                if src == dst {
                    continue;
                }
                let g = self.gain(src, dst);
                if g == 0.0 {
                    continue;
                }
                for (a, &s) in acc.iter_mut().zip(pcm.iter()) {
                    *a += (s as f32 * g) as i32;
                }
            }
            let mut mixed: PcmBuf = acc
                .iter()
                .map(|&v| v.clamp(i16::MIN as i32, i16::MAX as i32) as i16)
                .collect();
            mixed.resize(frame, 0);
            let dst_rate = self.taps[&dst].rate();
            let dst_pcm = if room_rate != dst_rate {
                resample(&mixed, room_rate, dst_rate)
            } else {
                mixed
            };
            let codec = self.taps[&dst].codec;
            let payload = self.taps.get_mut(&dst).unwrap().encoder.encode(&dst_pcm);
            out.insert(dst, Packet { codec, payload });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pcmu(value: i16, len: usize) -> Vec<u8> {
        let mut enc = create_encoder(CodecType::PCMU);
        enc.encode(&vec![value; len])
    }

    fn decode_mean(codec: CodecType, payload: &[u8]) -> f64 {
        let mut dec = create_decoder(codec);
        let pcm = dec.decode(payload);
        if pcm.is_empty() {
            return 0.0;
        }
        pcm.iter().map(|&s| s as f64).sum::<f64>() / pcm.len() as f64
    }

    #[test]
    fn single_source_same_codec_is_passthrough() {
        let mut m = UnifiedMixer::new(8000);
        let a = m.add_tap(CodecType::PCMU);
        let b = m.add_tap(CodecType::PCMU);
        let payload = pcmu(1000, 160);

        let out = m.on_inbound(a, payload.clone());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, b);
        // Byte-identical: no decode/encode happened.
        assert_eq!(out[0].1.payload, payload);
    }

    #[test]
    fn muting_a_source_silences_it_into_the_other_leg() {
        let mut m = UnifiedMixer::new(8000);
        let a = m.add_tap(CodecType::PCMU);
        let _b = m.add_tap(CodecType::PCMU);

        // Normally A -> B passes through.
        assert_eq!(m.on_inbound(a, pcmu(1000, 160)).len(), 1);

        // Muted: nothing reaches B.
        m.mute_source(a);
        assert!(m.on_inbound(a, pcmu(1000, 160)).is_empty(), "muted source emits nothing");

        // Unmuted: passthrough resumes.
        m.unmute_source(a);
        assert_eq!(m.on_inbound(a, pcmu(1000, 160)).len(), 1);
    }

    #[test]
    fn supervisor_whisper_is_heard_by_one_sink_only() {
        // C(aller), A(gent), S(upervisor). S whispers to A only: gain(S->C)=0.
        let mut m = UnifiedMixer::new(8000);
        let c = m.add_tap(CodecType::PCMU);
        let a = m.add_tap(CodecType::PCMU);
        let s = m.add_tap(CodecType::PCMU);
        m.set_gain(s, c, 0.0);

        // Only the supervisor is talking this frame.
        m.feed(s, pcmu(1000, 160));
        let out = m.mix_frame();

        let c_mean = decode_mean(CodecType::PCMU, &out[&c].payload).abs();
        let a_mean = decode_mean(CodecType::PCMU, &out[&a].payload).abs();
        assert!(c_mean < 100.0, "caller does NOT hear the whisper (mean {c_mean})");
        assert!(a_mean > 500.0, "agent DOES hear the whisper (mean {a_mean})");
    }

    #[test]
    fn whisper_stays_inaudible_to_a_later_joining_leg() {
        // S whispers to A: default-mute S, override S->A audible. A leg C that
        // joins AFTER must still not hear S.
        let mut m = UnifiedMixer::new(8000);
        let a = m.add_tap(CodecType::PCMU);
        let s = m.add_tap(CodecType::PCMU);
        m.set_source_default(s, 0.0); // S muted to all by default
        m.set_gain(s, a, 1.0); // ...except A

        // C joins late.
        let c = m.add_tap(CodecType::PCMU);
        m.feed(s, pcmu(1500, 160));
        let out = m.mix_frame();

        assert!(
            decode_mean(CodecType::PCMU, &out[&a].payload).abs() > 500.0,
            "A hears the whisper"
        );
        assert!(
            decode_mean(CodecType::PCMU, &out[&c].payload).abs() < 100.0,
            "the late-joining C does NOT hear the whisper"
        );
    }

    #[test]
    fn single_source_different_codec_transcodes() {
        let mut m = UnifiedMixer::new(8000);
        let a = m.add_tap(CodecType::PCMU);
        let _b = m.add_tap(CodecType::PCMA);

        let out = m.on_inbound(a, pcmu(1000, 160));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1.codec, CodecType::PCMA);
        let mean = decode_mean(CodecType::PCMA, &out[0].1.payload);
        assert!((mean - 1000.0).abs() < 200.0, "transcoded mean {mean}");
    }

    #[test]
    fn multi_source_yields_nothing_on_event_path() {
        // 3 participants → every dst has >1 source → on_inbound emits nothing;
        // mixing is the clock's job.
        let mut m = UnifiedMixer::new(8000);
        let a = m.add_tap(CodecType::PCMU);
        let _b = m.add_tap(CodecType::PCMU);
        let _c = m.add_tap(CodecType::PCMU);
        assert!(m.on_inbound(a, pcmu(1000, 160)).is_empty());
    }

    #[test]
    fn mix_frame_sums_sources_minus_self() {
        let mut m = UnifiedMixer::new(8000);
        let a = m.add_tap(CodecType::PCMU);
        let b = m.add_tap(CodecType::PCMU);
        let c = m.add_tap(CodecType::PCMU);
        m.feed(a, pcmu(1000, 160));
        m.feed(b, pcmu(1000, 160));

        let out = m.mix_frame();
        // C hears A+B → ~2000.
        assert!(decode_mean(CodecType::PCMU, &out[&c].payload) > 1500.0);
        // A hears only B → ~1000.
        assert!(decode_mean(CodecType::PCMU, &out[&a].payload) < 1500.0);
        assert!(decode_mean(CodecType::PCMU, &out[&a].payload) > 500.0);
    }

    #[test]
    fn recorder_tap_receives_room_mix() {
        let mut m = UnifiedMixer::new(8000);
        let a = m.add_tap(CodecType::PCMU);
        let b = m.add_tap(CodecType::PCMU);
        let rec = m.add_recorder(CodecType::PCMU);
        assert_eq!(m.role(rec), Some(TapRole::Recorder));
        m.feed(a, pcmu(1000, 160));
        m.feed(b, pcmu(1000, 160));

        let out = m.mix_frame();
        // Recorder hears everyone (A+B) → ~2000.
        assert!(decode_mean(CodecType::PCMU, &out[&rec].payload) > 1500.0);
    }

    #[test]
    fn long_packet_spans_multiple_mix_frames() {
        // A 40 ms packet (320 samples) must feed two 20 ms output frames — proves
        // no packet-duration assumption in the mix path.
        let mut m = UnifiedMixer::new(8000);
        let a = m.add_tap(CodecType::PCMU);
        let _b = m.add_tap(CodecType::PCMU); // silent
        let c = m.add_tap(CodecType::PCMU);
        m.feed(a, pcmu(1000, 320)); // one 40 ms packet

        let f1 = m.mix_frame();
        let f2 = m.mix_frame();
        let f3 = m.mix_frame();
        // C hears A in frames 1 and 2, then silence.
        assert!(decode_mean(CodecType::PCMU, &f1[&c].payload) > 500.0);
        assert!(decode_mean(CodecType::PCMU, &f2[&c].payload) > 500.0);
        assert!(decode_mean(CodecType::PCMU, &f3[&c].payload) < 200.0);
    }
}
