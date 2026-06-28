//! Pure, side-effect-free playback helpers extracted from the `SipSession`
//! god object (strangler-fig step 1).
//!
//! These functions own the logic where the last round of production
//! firefighting lived — most notably the caller-playback codec selection that
//! caused PCMA-pinned silence (fixed in `675ad891`). Because they are pure
//! functions of their inputs (SDP strings, file names, track ids) they are
//! exhaustively unit-testable in isolation, turning what used to be prod-only
//! bugs into deterministic regression tests.

use std::path::Path;

use anyhow::{Result, anyhow};
use audio_codec::CodecType;

use crate::call::domain::LegId;
use crate::media::negotiate::{CodecInfo, MediaNegotiator};

/// A concrete playback destination side. Resolved requests only ever target a
/// real leg (caller or callee); "both"/"dynamic" are request selectors, not
/// destinations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlaybackLeg {
    Caller,
    Callee,
}

impl PlaybackLeg {
    /// Canonical wire label. This is the single source of the leg's string
    /// identity — used for the `LegId`, track-id suffixes, and log fields.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            PlaybackLeg::Caller => "caller",
            PlaybackLeg::Callee => "callee",
        }
    }

    /// The bridge-endpoint leg id this side writes to.
    pub(crate) fn leg_id(self) -> LegId {
        LegId::from(self.label())
    }

    /// Caller is the only side that updates `bridge_playback_track_id`.
    pub(crate) fn is_caller(self) -> bool {
        matches!(self, PlaybackLeg::Caller)
    }
}

/// Which destination(s) a `handle_play` request selects, parsed once from the
/// stringly-typed `LegId` so the rest of the logic is fully enum-driven.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PlaybackRequest {
    /// `None` — caller only, track id without a leg suffix (back-compat).
    CallerDefault,
    /// Explicit caller leg (suffixed track id).
    Caller,
    /// Explicit callee leg.
    Callee,
    /// Both legs (whichever have a bridge).
    Both,
    /// Anything else — a dynamic leg, unsupported on this path.
    Dynamic(LegId),
}

impl PlaybackRequest {
    /// Parse the incoming optional `LegId`. This is the only place the
    /// caller/callee/both string identifiers are matched.
    fn classify(leg_id: Option<&LegId>) -> Self {
        match leg_id {
            None => PlaybackRequest::CallerDefault,
            Some(lid) => match lid.as_str() {
                "caller" => PlaybackRequest::Caller,
                "callee" => PlaybackRequest::Callee,
                "both" => PlaybackRequest::Both,
                _ => PlaybackRequest::Dynamic(lid.clone()),
            },
        }
    }
}

/// One resolved playback destination: the side-effect-free description of where
/// a single `FileTrack` should be written and how it should be registered.
///
/// The adapter (`SipSession`) consumes these to perform the actual bridge I/O
/// (`replace_output_with_file`) and track-registry bookkeeping. Keeping the
/// decision (which legs, which track ids, which errors) in a pure function
/// makes the leg-gating logic exhaustively unit-testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlaybackTarget {
    /// The destination side; carries its label, leg id, and caller-ness.
    pub leg: PlaybackLeg,
    /// The track id this playback registers under.
    pub track_id: String,
}

/// Decide which legs a `handle_play` request should write to, the track id for
/// each, and the bridge-gating errors — faithfully extracted from the old
/// `play_to_leg!` macro + match.
///
/// `caller_uses_bridge` / `callee_uses_bridge` mirror
/// `media.caller_answer_uses_media_bridge` / `media.callee_offer_uses_media_bridge`.
pub(crate) fn resolve_playback_targets(
    leg_id: Option<&LegId>,
    base_track_id: &str,
    caller_uses_bridge: bool,
    callee_uses_bridge: bool,
) -> Result<Vec<PlaybackTarget>> {
    let caller_target = |with_suffix: bool| PlaybackTarget {
        leg: PlaybackLeg::Caller,
        track_id: if with_suffix {
            format!("{base_track_id}-{}", PlaybackLeg::Caller.label())
        } else {
            base_track_id.to_string()
        },
    };
    let callee_target = || PlaybackTarget {
        leg: PlaybackLeg::Callee,
        track_id: format!("{base_track_id}-{}", PlaybackLeg::Callee.label()),
    };

    match PlaybackRequest::classify(leg_id) {
        PlaybackRequest::Caller => {
            if !caller_uses_bridge {
                return Err(anyhow!("Playback requires media bridge for caller leg"));
            }
            Ok(vec![caller_target(true)])
        }
        PlaybackRequest::Callee => {
            if !callee_uses_bridge {
                return Err(anyhow!("Playback requires media bridge for callee leg"));
            }
            Ok(vec![callee_target()])
        }
        // Both legs: play to whichever side(s) have a bridge; error only if
        // neither does.
        PlaybackRequest::Both => {
            let mut targets = Vec::new();
            if caller_uses_bridge {
                targets.push(caller_target(true));
            }
            if callee_uses_bridge {
                targets.push(callee_target());
            }
            if targets.is_empty() {
                return Err(anyhow!("No leg has media bridge for playback"));
            }
            Ok(targets)
        }
        PlaybackRequest::Dynamic(lid) => Err(anyhow!(
            "Playback to dynamic leg {} requires media bridge output mapping",
            lid
        )),
        // Caller only (backward compatible); no track-id suffix.
        PlaybackRequest::CallerDefault => {
            if !caller_uses_bridge {
                return Err(anyhow!("Playback requires media bridge for caller leg"));
            }
            Ok(vec![caller_target(false)])
        }
    }
}

/// Decide which registered playback track ids `handle_stop_playback` should
/// stop for a given target leg. Pure over the current track-id key set.
pub(crate) fn tracks_to_stop<'a, I>(track_ids: I, leg_id: Option<&LegId>) -> Vec<String>
where
    I: IntoIterator<Item = &'a String>,
{
    match leg_id {
        None => track_ids.into_iter().cloned().collect(),
        Some(lid) => {
            let suffix = format!("-{}", lid);
            let is_caller = lid.0 == "caller";
            track_ids
                .into_iter()
                .filter(|tid| {
                    tid.ends_with(&suffix)
                        || **tid == lid.0
                        || (is_caller && !tid.contains('-'))
                })
                .cloned()
                .collect()
        }
    }
}

/// Select the RTP codec to render caller-facing playback in.
///
/// Render playback in the codec the caller actually **negotiated** (its
/// answer), not merely the first codec it **offered**. When a single codec is
/// pinned (e.g. PCMA) the offer's first entry (often PCMU) differs from the
/// negotiated payload type; sending the offer-first codec makes the caller drop
/// every packet as an unknown PT — silent audio (the `675ad891` bug).
///
/// Resolution order:
/// 1. First audio codec from the negotiated answer SDP, if present.
/// 2. Else first audio codec from the caller's original offer SDP.
/// 3. Else fall back to PCMU.
pub(crate) fn select_playback_codec(
    answer_sdp: Option<&str>,
    caller_offer_sdp: Option<&str>,
) -> CodecInfo {
    answer_sdp
        .or(caller_offer_sdp)
        .map(|sdp| MediaNegotiator::extract_codec_params(sdp).audio)
        .and_then(|codecs| codecs.into_iter().next())
        .unwrap_or_else(|| MediaNegotiator::codec_info_for_type(CodecType::PCMU))
}

/// Resolve a configured audio file reference to a concrete path.
///
/// - Absolute/existing paths and `http(s)://` URLs pass through untouched.
/// - `config/` (or `./config/`) prefixed paths pass through.
/// - A bare name resolves against `config/` then `config/sounds/` (the queue UI
///   stores uploaded sounds under `config/sounds`).
/// - If nothing matches, the original string is returned unchanged.
pub(crate) fn resolve_audio_file_path(audio_file: &str) -> String {
    if audio_file.starts_with("http://") || audio_file.starts_with("https://") {
        return audio_file.to_string();
    }

    let path = Path::new(audio_file);
    if path.is_absolute() || path.exists() {
        return audio_file.to_string();
    }

    if audio_file.starts_with("config/") || audio_file.starts_with("./config/") {
        return audio_file.to_string();
    }

    let fallback = Path::new("config").join(audio_file);
    if fallback.exists() {
        return fallback.to_string_lossy().to_string();
    }

    // Bare filename (e.g. "anna_busy.mp3") or "sounds/x" that wasn't found
    // above: the queue UI stores uploaded sounds under config/sounds, so try
    // there before giving up.
    let in_sounds = Path::new("config/sounds").join(audio_file);
    if in_sounds.exists() {
        return in_sounds.to_string_lossy().to_string();
    }

    audio_file.to_string()
}

/// The leg a registered playback `track_id` belongs to, inferred from its
/// canonical suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TrackLeg {
    Caller,
    Callee,
    /// A dynamically-added leg, carrying its resolved `LegId`.
    Dynamic(LegId),
}

/// Infer which leg a playback `track_id` belongs to from its canonical suffix.
pub(crate) fn infer_track_leg(track_id: &str) -> TrackLeg {
    if track_id.ends_with("-caller") || track_id == "caller" {
        TrackLeg::Caller
    } else if track_id.ends_with("-callee") || track_id == "callee" {
        TrackLeg::Callee
    } else if let Some(pos) = track_id.rfind("-leg-") {
        // Only the leading '-' is stripped, so the id retains the "leg-" prefix.
        TrackLeg::Dynamic(LegId::new(&track_id[pos + 1..]))
    } else {
        TrackLeg::Caller // fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal SDP with the given audio `m=` payload-type list and rtpmaps for
    /// PCMU(0)/PCMA(8)/telephone-event(101).
    fn sdp_with_audio(pts: &str) -> String {
        format!(
            "v=0\r\n\
             o=- 0 0 IN IP4 127.0.0.1\r\n\
             s=-\r\n\
             c=IN IP4 127.0.0.1\r\n\
             t=0 0\r\n\
             m=audio 5004 RTP/AVP {pts}\r\n\
             a=rtpmap:0 PCMU/8000\r\n\
             a=rtpmap:8 PCMA/8000\r\n\
             a=rtpmap:101 telephone-event/8000\r\n"
        )
    }

    #[test]
    fn regression_pcma_pinned_answer_wins_over_offer_first() {
        // The 675ad891 bug: caller OFFERED `0 8 101` (PCMU first) but the
        // negotiated ANSWER pinned `8` (PCMA). Playback must use PT 8, not 0,
        // or the caller drops every packet as an unknown PT (silence).
        let offer = sdp_with_audio("0 8 101");
        let answer = sdp_with_audio("8");

        let chosen = select_playback_codec(Some(&answer), Some(&offer));

        assert_eq!(chosen.payload_type, 8, "must use negotiated PCMA, not offered PCMU");
        assert_eq!(chosen.codec, CodecType::PCMA);
    }

    #[test]
    fn falls_back_to_offer_when_no_answer() {
        let offer = sdp_with_audio("8 0 101");
        let chosen = select_playback_codec(None, Some(&offer));
        assert_eq!(chosen.payload_type, 8);
        assert_eq!(chosen.codec, CodecType::PCMA);
    }

    #[test]
    fn answer_first_audio_codec_is_selected() {
        let answer = sdp_with_audio("0 8 101");
        let chosen = select_playback_codec(Some(&answer), None);
        assert_eq!(chosen.payload_type, 0);
        assert_eq!(chosen.codec, CodecType::PCMU);
    }

    #[test]
    fn defaults_to_pcmu_when_nothing_available() {
        let chosen = select_playback_codec(None, None);
        assert_eq!(chosen.codec, CodecType::PCMU);
        assert_eq!(chosen.payload_type, CodecType::PCMU.payload_type());
    }

    #[test]
    fn empty_or_audioless_sdp_falls_back_to_pcmu() {
        let no_audio = "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n";
        let chosen = select_playback_codec(Some(no_audio), None);
        assert_eq!(chosen.codec, CodecType::PCMU);
    }

    #[test]
    fn dtmf_only_is_not_picked_as_audio() {
        // telephone-event is classified as dtmf, never audio, so it must not be
        // selected for playback even if listed first.
        let answer = sdp_with_audio("101 8");
        let chosen = select_playback_codec(Some(&answer), None);
        assert_eq!(chosen.codec, CodecType::PCMA);
        assert_eq!(chosen.payload_type, 8);
    }

    #[test]
    fn resolve_passes_through_urls_and_config_prefixes() {
        assert_eq!(
            resolve_audio_file_path("https://x/y.mp3"),
            "https://x/y.mp3"
        );
        assert_eq!(
            resolve_audio_file_path("http://x/y.mp3"),
            "http://x/y.mp3"
        );
        assert_eq!(resolve_audio_file_path("config/a.wav"), "config/a.wav");
        assert_eq!(resolve_audio_file_path("./config/a.wav"), "./config/a.wav");
    }

    #[test]
    fn resolve_unknown_bare_name_returns_unchanged() {
        // A name that exists in neither config/ nor config/sounds/ is returned
        // verbatim (caller decides what to do with a missing file).
        let name = "definitely_missing_4f3a.wav";
        assert_eq!(resolve_audio_file_path(name), name);
    }

    fn tids(t: &[PlaybackTarget]) -> Vec<(&'static str, String, bool)> {
        t.iter()
            .map(|t| (t.leg.label(), t.track_id.clone(), t.leg.is_caller()))
            .collect()
    }

    #[test]
    fn resolve_none_is_caller_only_without_suffix() {
        let t = resolve_playback_targets(None, "greeting", true, false).unwrap();
        assert_eq!(tids(&t), vec![("caller", "greeting".to_string(), true)]);
        assert_eq!(t[0].leg.leg_id(), LegId::from("caller"));
    }

    #[test]
    fn resolve_explicit_caller_gets_suffix() {
        let t = resolve_playback_targets(Some(&LegId::from("caller")), "g", true, false).unwrap();
        assert_eq!(tids(&t), vec![("caller", "g-caller".to_string(), true)]);
    }

    #[test]
    fn resolve_callee_requires_callee_bridge() {
        let ok = resolve_playback_targets(Some(&LegId::from("callee")), "g", false, true).unwrap();
        assert_eq!(tids(&ok), vec![("callee", "g-callee".to_string(), false)]);

        let err = resolve_playback_targets(Some(&LegId::from("callee")), "g", true, false);
        assert!(err.unwrap_err().to_string().contains("callee leg"));
    }

    #[test]
    fn resolve_caller_requires_caller_bridge() {
        let err = resolve_playback_targets(Some(&LegId::from("caller")), "g", false, true);
        assert!(err.unwrap_err().to_string().contains("caller leg"));
        // None path uses the same caller-leg error.
        let err2 = resolve_playback_targets(None, "g", false, false);
        assert!(err2.unwrap_err().to_string().contains("caller leg"));
    }

    #[test]
    fn resolve_both_plays_available_sides_only() {
        // Both available -> caller first, then callee (order preserved).
        let both = resolve_playback_targets(Some(&LegId::from("both")), "g", true, true).unwrap();
        assert_eq!(
            tids(&both),
            vec![
                ("caller", "g-caller".to_string(), true),
                ("callee", "g-callee".to_string(), false),
            ]
        );
        // Only caller available -> just caller, no error.
        let c = resolve_playback_targets(Some(&LegId::from("both")), "g", true, false).unwrap();
        assert_eq!(tids(&c), vec![("caller", "g-caller".to_string(), true)]);
        // Only callee available -> just callee.
        let cl = resolve_playback_targets(Some(&LegId::from("both")), "g", false, true).unwrap();
        assert_eq!(tids(&cl), vec![("callee", "g-callee".to_string(), false)]);
        // Neither -> error.
        let err = resolve_playback_targets(Some(&LegId::from("both")), "g", false, false);
        assert!(err.unwrap_err().to_string().contains("No leg has media bridge"));
    }

    #[test]
    fn resolve_dynamic_leg_is_unsupported() {
        let err = resolve_playback_targets(Some(&LegId::from("leg-7")), "g", true, true);
        assert!(err.unwrap_err().to_string().contains("dynamic leg"));
    }

    #[test]
    fn tracks_to_stop_none_returns_all() {
        let keys = vec!["a".to_string(), "b-callee".to_string(), "playback".to_string()];
        let mut got = tracks_to_stop(keys.iter(), None);
        got.sort();
        assert_eq!(got, vec!["a", "b-callee", "playback"]);
    }

    #[test]
    fn tracks_to_stop_caller_matches_bare_and_suffix() {
        let keys = vec![
            "greeting".to_string(),       // bare, no '-': caller match
            "greeting-caller".to_string(), // suffix match
            "greeting-callee".to_string(), // callee, excluded
            "caller".to_string(),          // == lid.0
        ];
        let mut got = tracks_to_stop(keys.iter(), Some(&LegId::from("caller")));
        got.sort();
        assert_eq!(got, vec!["caller", "greeting", "greeting-caller"]);
    }

    #[test]
    fn tracks_to_stop_callee_only_suffix_or_exact() {
        let keys = vec![
            "greeting".to_string(),        // bare: NOT a callee match
            "greeting-callee".to_string(), // suffix match
            "callee".to_string(),          // == lid.0
        ];
        let mut got = tracks_to_stop(keys.iter(), Some(&LegId::from("callee")));
        got.sort();
        assert_eq!(got, vec!["callee", "greeting-callee"]);
    }

    #[test]
    fn infer_track_leg_covers_all_shapes() {
        assert_eq!(infer_track_leg("playback-caller"), TrackLeg::Caller);
        assert_eq!(infer_track_leg("playback-callee"), TrackLeg::Callee);
        assert_eq!(infer_track_leg("caller"), TrackLeg::Caller);
        assert_eq!(infer_track_leg("callee"), TrackLeg::Callee);
        // Note: only the leading '-' is stripped, so the id retains the
        // "leg-" prefix — preserved verbatim from the original implementation.
        assert_eq!(
            infer_track_leg("pb-leg-abc123"),
            TrackLeg::Dynamic(LegId::from("leg-abc123"))
        );
        // Unknown shape falls back to caller.
        assert_eq!(infer_track_leg("playback"), TrackLeg::Caller);
    }
}
