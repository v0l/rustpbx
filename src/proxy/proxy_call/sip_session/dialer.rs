//! Pure outbound-dial decision logic extracted from the `SipSession` god
//! object (strangler-fig step 3, the Dialer).
//!
//! The async orchestration of an outbound INVITE (the `select!` over the
//! caller-end signal, `cancel_token`, the `do_invite` future and the
//! early-media relay) stays in the `SipSession` adapter for now. What lives
//! here is the side-effect-free decision surface that governed the gnarliest,
//! least-tested branches of `try_single_target`:
//!
//! * the `422 Session Interval Too Small` retry predicate, and
//! * the INVITE header rewrite that retry performs.
//!
//! Both are pure functions of their inputs, so the behaviour that previously
//! could only be exercised against a live registrar is now unit-testable.

use rsipstack::sip::Header;

use crate::proxy::proxy_call::session_timer::{
    build_default_session_timer_headers, HEADER_MIN_SE, HEADER_SESSION_EXPIRES, HEADER_SUPPORTED,
};

/// Decide whether to re-send an INVITE after a `422 Session Interval Too Small`.
///
/// We retry at most once, and only when session timers are enabled and the
/// rejection actually was a 422. `retry_count` is the number of retries already
/// performed.
pub(crate) fn should_retry_min_se(
    timer_enabled: bool,
    is_interval_too_small: bool,
    retry_count: u32,
) -> bool {
    timer_enabled && is_interval_too_small && retry_count < 1
}

/// Rewrite an INVITE's headers for a `Min-SE` retry: strip any existing
/// `Session-Expires`/`Min-SE`, drop `timer` from `Supported`, then re-add timer
/// headers pinned to the registrar-demanded `min_se` (used for both the session
/// interval and Min-SE). Faithfully mirrors the original inline block.
pub(crate) fn rewrite_invite_headers_for_min_se(headers: &mut Vec<Header>, min_se_secs: u64) {
    // Drop any pre-existing Other Session-Expires / Min-SE headers.
    headers.retain(|header| {
        !matches!(header,
            Header::Other(name, _)
                if name.eq_ignore_ascii_case(HEADER_SESSION_EXPIRES)
                    || name.eq_ignore_ascii_case(HEADER_MIN_SE))
    });

    // Drop the `timer` option-tag from any typed Supported header, re-emitting
    // it as an Other header carrying the filtered list.
    //
    // KNOWN LATENT BUG (preserved verbatim from the original): the typed
    // `Supported` header's `Display` is `"Supported: <value>"`, so `to_string()`
    // includes the header *name*. Splitting on ',' then yields a first entry
    // like `"Supported: timer"`, which never equals `"timer"`, so the `timer`
    // tag is NOT actually stripped from a typed Supported header. Using
    // `value.value()` here would fix it, but that is a behavioural change on a
    // thinly-covered shared signalling path and is intentionally NOT made as
    // part of this extraction. See `rewrite_keeps_timer_on_typed_supported`.
    for header in headers.iter_mut() {
        if let Header::Supported(value) = header {
            let filtered: Vec<String> = value
                .to_string()
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty() && *entry != "timer")
                .map(ToString::to_string)
                .collect();
            *header = Header::Other(HEADER_SUPPORTED.to_string(), filtered.join(", "));
        }
    }

    // Remove now-empty Supported headers and any leftover Session-Expires/Min-SE.
    headers.retain(|header| match header {
        Header::Other(name, value) if name.eq_ignore_ascii_case(HEADER_SUPPORTED) => {
            !value.trim().is_empty()
        }
        Header::Other(name, _) => {
            !name.eq_ignore_ascii_case(HEADER_SESSION_EXPIRES)
                && !name.eq_ignore_ascii_case(HEADER_MIN_SE)
        }
        _ => true,
    });

    headers.extend(build_default_session_timer_headers(min_se_secs, min_se_secs));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_names(headers: &[Header]) -> Vec<String> {
        headers.iter().map(|h| h.name().to_string()).collect()
    }

    fn other(name: &str, value: &str) -> Header {
        Header::Other(name.to_string(), value.to_string())
    }

    #[test]
    fn retry_only_once_and_only_for_422_with_timers() {
        assert!(should_retry_min_se(true, true, 0));
        // already retried once
        assert!(!should_retry_min_se(true, true, 1));
        // not a 422
        assert!(!should_retry_min_se(true, false, 0));
        // timers disabled
        assert!(!should_retry_min_se(false, true, 0));
    }

    #[test]
    fn rewrite_strips_old_timer_headers_and_pins_min_se() {
        let mut headers = vec![
            other("Session-Expires", "90"),
            other("Min-SE", "60"),
            other("Contact", "<sip:a@b>"),
        ];
        rewrite_invite_headers_for_min_se(&mut headers, 1800);

        let names = header_names(&headers);
        // The old, conflicting timer values must be gone exactly once...
        assert_eq!(
            names.iter().filter(|n| n.eq_ignore_ascii_case("Session-Expires")).count(),
            1,
            "exactly one fresh Session-Expires"
        );
        // ...and the unrelated header survives.
        assert!(names.iter().any(|n| n.eq_ignore_ascii_case("Contact")));

        // The fresh timer headers carry the demanded Min-SE (1800).
        let se = headers.iter().find_map(|h| match h {
            Header::Other(n, v) if n.eq_ignore_ascii_case("Session-Expires") => Some(v.clone()),
            _ => None,
        });
        assert!(se.unwrap().contains("1800"));
    }

    #[test]
    fn rewrite_keeps_timer_on_typed_supported_latent_bug() {
        // Characterization of the KNOWN LATENT BUG: because the typed Supported
        // Display prefixes "Supported: ", the `timer` tag is not stripped. This
        // test pins the *actual* current behaviour so a future intentional fix
        // is a deliberate, visible change.
        let mut headers = vec![Header::Supported("timer, 100rel".into())];
        rewrite_invite_headers_for_min_se(&mut headers, 1800);

        let supported = headers.iter().find_map(|h| match h {
            Header::Other(n, v) if n.eq_ignore_ascii_case(HEADER_SUPPORTED) => Some(v.clone()),
            _ => None,
        });
        // Re-emitted as Other, and (bug) still contains "timer".
        let supported = supported.expect("Supported retained as Other");
        assert!(
            supported.contains("timer"),
            "latent bug: timer NOT stripped from typed Supported (got {supported:?})"
        );
        assert!(supported.contains("100rel"));
    }

    #[test]
    fn rewrite_leaves_other_supported_untouched() {
        // The realistic production shape: Supported as an Other header. The
        // timer-stripping loop only matches the typed variant, so an Other
        // Supported is preserved as-is (non-empty), unaffected by the rewrite.
        let mut headers = vec![other("Supported", "timer, 100rel")];
        rewrite_invite_headers_for_min_se(&mut headers, 1800);
        let supported = headers.iter().find_map(|h| match h {
            Header::Other(n, v) if n.eq_ignore_ascii_case(HEADER_SUPPORTED) => Some(v.clone()),
            _ => None,
        });
        assert_eq!(supported.as_deref(), Some("timer, 100rel"));
    }
}
