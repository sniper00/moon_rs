use crate::actor::LuaActor;
use crate::context::ActorId;
use futures_util::{TryStream, TryStreamExt};
use moon_base::laux::{self, LuaState};
use std::ffi::c_int;
use tokio::sync::oneshot;

pub(crate) enum CursorSignal {
    Next(ActorId, i64),
    Close,
}

pub(crate) type CursorSender = oneshot::Sender<CursorSignal>;

pub(crate) struct CursorHandle(pub(crate) Option<CursorSender>);

impl CursorHandle {
    pub(crate) fn next(&mut self, state: LuaState, prefix: &str) -> c_int {
        let Some(tx) = self.0.take() else {
            return crate::lua_push_error_tuple(
                state,
                &format!("{prefix}: already consumed or closed"),
            );
        };
        let actor = LuaActor::from_lua_state(state);
        let owner = unsafe { (*actor).id };
        let session = unsafe { (*actor).next_session() };
        if tx.send(CursorSignal::Next(owner, session)).is_err() {
            return crate::lua_push_error_tuple(
                state,
                &format!("{prefix}: stream handler is gone"),
            );
        }
        laux::lua_push(state, session);
        1
    }

    pub(crate) fn close(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(CursorSignal::Close);
        }
    }
}

impl Drop for CursorHandle {
    fn drop(&mut self) {
        self.close();
    }
}

/// A full batch carries a one-use continuation. An error discards the partial
/// batch and is returned with the owner/session currently waiting for it.
pub(crate) async fn stream_batches<S, F>(
    mut stream: S,
    batch_size: usize,
    mut owner: ActorId,
    mut session: i64,
    mut send: F,
) -> Result<(), (ActorId, i64, S::Error)>
where
    S: TryStream + Unpin,
    F: FnMut(ActorId, i64, Vec<S::Ok>, Option<CursorSender>) -> bool,
{
    debug_assert!(batch_size > 0);
    loop {
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            match stream.try_next().await {
                Ok(Some(row)) => batch.push(row),
                Ok(None) => break,
                Err(err) => return Err((owner, session, err)),
            }
        }
        let (next_tx, next_rx) = if batch.len() == batch_size {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        if !send(owner, session, batch, next_tx) {
            return Ok(());
        }
        let Some(next_rx) = next_rx else {
            return Ok(());
        };
        match next_rx.await {
            Ok(CursorSignal::Next(next_owner, next_session)) => {
                owner = next_owner;
                session = next_session;
            }
            _ => return Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/db_cursor.rs"]
mod tests;
