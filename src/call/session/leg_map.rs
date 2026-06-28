//! A call's **leg-identity layer**: maps the external [`LegId`] namespace (what
//! RWI/console/AMI commands and CDR/leg events speak) to the internal handles a
//! leg owns — its switch [`PortId`] and its mixer [`TapId`].
//!
//! The new engine works in `PortId`/`TapId`/`TargetIdx`; external automation
//! works in `LegId` strings. This map is the single translation point, so a
//! command like `ConferenceMute { leg_id }` resolves to the right mixer tap, and
//! the engine can emit leg-lifecycle events under stable ids.

use std::collections::HashMap;

use super::switch::PortId;
use crate::call::domain::LegId;
use crate::media::unified_mixer::TapId;

/// What kind of party a leg is, so callers can find e.g. "the caller" without
/// knowing its id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegRole {
    Caller,
    Callee,
    Supervisor,
}

#[derive(Debug, Clone)]
struct LegEntry {
    port: PortId,
    /// The mixer tap, if the leg has joined the bridge (a not-yet-bridged callee
    /// has a port but no tap).
    tap: Option<TapId>,
    role: LegRole,
}

/// The legs of one call, keyed by their external [`LegId`].
#[derive(Default)]
pub struct LegMap {
    legs: HashMap<LegId, LegEntry>,
}

impl LegMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or update) a leg's identity. `tap` is `None` until it joins the
    /// bridge.
    pub fn insert(&mut self, leg_id: LegId, port: PortId, tap: Option<TapId>, role: LegRole) {
        self.legs.insert(leg_id, LegEntry { port, tap, role });
    }

    /// Attach (or replace) a leg's mixer tap once it joins the bridge.
    pub fn set_tap(&mut self, leg_id: &LegId, tap: TapId) {
        if let Some(e) = self.legs.get_mut(leg_id) {
            e.tap = Some(tap);
        }
    }

    pub fn remove(&mut self, leg_id: &LegId) {
        self.legs.remove(leg_id);
    }

    /// The mixer tap for a leg, if it has one.
    pub fn tap(&self, leg_id: &LegId) -> Option<TapId> {
        self.legs.get(leg_id).and_then(|e| e.tap)
    }

    /// The switch slot (PortId) for a leg.
    pub fn port(&self, leg_id: &LegId) -> Option<PortId> {
        self.legs.get(leg_id).map(|e| e.port)
    }

    pub fn role(&self, leg_id: &LegId) -> Option<LegRole> {
        self.legs.get(leg_id).map(|e| e.role)
    }

    /// The id of the (first) leg with a given role — e.g. the caller.
    pub fn by_role(&self, role: LegRole) -> Option<LegId> {
        self.legs
            .iter()
            .find(|(_, e)| e.role == role)
            .map(|(id, _)| id.clone())
    }

    pub fn len(&self) -> usize {
        self.legs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.legs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_leg_ids_to_taps_and_ports() {
        let mut m = LegMap::new();
        m.insert(LegId::new("caller"), PortId(0), Some(TapId(1)), LegRole::Caller);
        // A callee dialed but not yet bridged: port, no tap.
        m.insert(LegId::new("callee-0"), PortId(1), None, LegRole::Callee);

        assert_eq!(m.tap(&LegId::new("caller")), Some(TapId(1)));
        assert_eq!(m.port(&LegId::new("caller")), Some(PortId(0)));
        assert_eq!(m.tap(&LegId::new("callee-0")), None, "unbridged callee has no tap yet");

        // Once it bridges, the tap is attached.
        m.set_tap(&LegId::new("callee-0"), TapId(2));
        assert_eq!(m.tap(&LegId::new("callee-0")), Some(TapId(2)));

        assert_eq!(m.by_role(LegRole::Caller), Some(LegId::new("caller")));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn unknown_leg_resolves_to_nothing() {
        let m = LegMap::new();
        assert_eq!(m.tap(&LegId::new("ghost")), None);
        assert_eq!(m.by_role(LegRole::Supervisor), None);
    }

    #[test]
    fn removing_a_leg_drops_its_identity() {
        let mut m = LegMap::new();
        m.insert(LegId::new("callee-0"), PortId(1), Some(TapId(2)), LegRole::Callee);
        m.remove(&LegId::new("callee-0"));
        assert!(m.is_empty());
    }
}
