//! Maps a live call's session id to its command sink, so external producers
//! (RWI / console / AMI) can deliver a [`CallCommand`] to the right running call
//! and await its [`CommandResult`].
//!
//! A call registers its sink when it starts and unregisters when it ends (via an
//! RAII [`CommandGuard`]). [`CommandRegistry::dispatch`] looks the call up, sends
//! the command with a reply channel, and awaits the result — the producer side
//! of the live-control seam wired in [`graph_runner`](super::graph_runner).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::{mpsc, oneshot};

use super::graph_runner::GraphCommand;
use crate::call::domain::CallCommand;
use crate::call::runtime::CommandResult;

/// A shared, cloneable map of `session_id -> command sink`.
#[derive(Clone, Default)]
pub struct CommandRegistry {
    inner: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<GraphCommand>>>>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a live call's command sink. The returned guard unregisters the
    /// session on drop (i.e. when the call ends).
    #[must_use]
    pub fn register(
        &self,
        session_id: impl Into<String>,
        tx: mpsc::UnboundedSender<GraphCommand>,
    ) -> CommandGuard {
        let id = session_id.into();
        self.inner.lock().unwrap().insert(id.clone(), tx);
        CommandGuard {
            registry: self.clone(),
            id,
        }
    }

    pub fn unregister(&self, session_id: &str) {
        self.inner.lock().unwrap().remove(session_id);
    }

    /// Whether a live call with this id is currently registered.
    pub fn contains(&self, session_id: &str) -> bool {
        self.inner.lock().unwrap().contains_key(session_id)
    }

    /// Deliver a [`CallCommand`] to the live call and await its result.
    pub async fn dispatch(&self, session_id: &str, cmd: CallCommand) -> CommandResult {
        let tx = self.inner.lock().unwrap().get(session_id).cloned();
        let Some(tx) = tx else {
            return CommandResult::not_supported("no live call with that session id");
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        if tx.send((cmd, reply_tx)).is_err() {
            return CommandResult::failure("the call's command channel is closed");
        }
        match reply_rx.await {
            Ok(result) => result,
            Err(_) => CommandResult::failure("the call ended before replying"),
        }
    }
}

/// RAII guard: unregisters the session when dropped (the call ended).
pub struct CommandGuard {
    registry: CommandRegistry,
    id: String,
}

impl Drop for CommandGuard {
    fn drop(&mut self) {
        self.registry.unregister(&self.id);
    }
}

/// The process-wide registry the proxy's `serve`/`serve_graph` register into and
/// that RWI/console/AMI dispatch through.
pub fn global() -> &'static CommandRegistry {
    static REGISTRY: OnceLock<CommandRegistry> = OnceLock::new();
    REGISTRY.get_or_init(CommandRegistry::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::call::domain::HangupCommand;

    #[tokio::test]
    async fn dispatch_reaches_the_registered_call_and_returns_its_result() {
        let reg = CommandRegistry::new();
        let (tx, mut rx) = mpsc::unbounded_channel::<GraphCommand>();
        let _guard = reg.register("call-1", tx);
        assert!(reg.contains("call-1"));

        // Stand in for the live call: reply success to whatever arrives.
        tokio::spawn(async move {
            while let Some((_cmd, reply)) = rx.recv().await {
                let _ = reply.send(CommandResult::success());
            }
        });

        let result = reg
            .dispatch("call-1", CallCommand::Hangup(HangupCommand::all(None, None)))
            .await;
        assert!(result.success, "the command reached the live call");
    }

    #[tokio::test]
    async fn dispatch_to_unknown_session_is_not_supported() {
        let reg = CommandRegistry::new();
        let result = reg
            .dispatch("ghost", CallCommand::Hangup(HangupCommand::all(None, None)))
            .await;
        assert!(!result.success, "unknown session yields a non-success result");
    }

    #[test]
    fn guard_unregisters_on_drop() {
        let reg = CommandRegistry::new();
        let (tx, _rx) = mpsc::unbounded_channel::<GraphCommand>();
        {
            let _guard = reg.register("call-2", tx);
            assert!(reg.contains("call-2"));
        }
        assert!(!reg.contains("call-2"), "dropping the guard unregistered the call");
    }
}
