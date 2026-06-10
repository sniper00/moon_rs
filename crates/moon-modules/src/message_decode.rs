//! Centralized message decode registry and helpers.

use moon_lua::laux::LuaState;
use moon_runtime::{
    buffer::Buffer,
    context::{Message, MessageBody},
};

use crate::lua_push_error;

/// Decode a runtime `Message` into Lua stack values.
/// The decoder must consume the message payload (see `Message::take_body`).
pub type MessageDecodeFn = unsafe extern "C-unwind" fn(LuaState, *mut Message) -> i32;

pub(crate) unsafe extern "C-unwind" fn default_decode(_state: LuaState, m: *mut Message) -> i32 {
    log::error!("no message decoder registered for ptype {}", unsafe {
        (*m).ptype()
    });
    0
}

/// Take a `BoxedValue` payload and downcast to `T`.
pub unsafe fn take_boxed<T: Send>(m: *mut Message) -> Result<T, String> {
    unsafe {
        let body = (*m).take_body();
        match body {
            MessageBody::Boxed(_, mut boxed) => {
                let ptr = boxed.into_raw();
                if ptr.is_null() {
                    return Err("boxed message payload already consumed".to_string());
                }
                Ok(*Box::from_raw(ptr as *mut T))
            }
            other => {
                let ptype = (*m).ptype();
                (*m).data = other;
                Err(format!(
                    "expected Boxed message body for ptype {}, got {}",
                    ptype,
                    body_discriminant(&(*m).data)
                ))
            }
        }
    }
}

/// Borrow the `Buffer` payload **without** taking ownership of it.
///
/// The body is left inside the `Message`, so the `Message`'s own `Drop` still
/// frees the buffer. Use this for any decoder whose body-reading path can
/// `longjmp` out via a Lua error (e.g. a malformed `seri` stream, or a Lua
/// allocation failure while pushing): a taken `Box<Buffer>` would leak because
/// `longjmp` skips Rust destructors, whereas a borrowed buffer is freed by the
/// owner of the `Message` (whose frame is *not* unwound by the `longjmp` back to
/// the enclosing `pcall`), on both the success and error paths.
///
/// # Safety
/// `m` must point to a live `Message`; the returned reference must not outlive it,
/// and the `Message` must not be mutated while the reference is held.
pub unsafe fn borrow_buffer<'a>(m: *mut Message) -> Result<&'a Buffer, String> {
    unsafe {
        match &(*m).data {
            MessageBody::Buffer(_, buf) => Ok(&**buf),
            other => Err(format!(
                "expected Buffer message body for ptype {}, got {}",
                (*m).ptype(),
                body_discriminant(other)
            )),
        }
    }
}

fn body_discriminant(body: &MessageBody) -> &'static str {
    match body {
        MessageBody::ISize(..) => "ISize",
        MessageBody::Buffer(..) => "Buffer",
        MessageBody::Boxed(..) => "Boxed",
        MessageBody::None(..) => "None",
    }
}

pub unsafe extern "C-unwind" fn decode_error_message(state: LuaState, m: *mut Message) -> i32 {
    unsafe {
        // Borrow (don't take): `lua_push` can longjmp on Lua OOM, which would skip
        // the `Drop` of a taken `Box<Buffer>` and leak it.
        match borrow_buffer(m) {
            Ok(buf) => {
                moon_lua::laux::lua_push(state, false);
                // Lossless for valid UTF-8; for invalid bytes preserve the rest of
                // the message with U+FFFD substitution instead of dropping it all.
                let message = String::from_utf8_lossy(buf.as_slice());
                moon_lua::laux::lua_push(state, message.as_ref());
                2
            }
            Err(e) => lua_push_error(state, &e),
        }
    }
}

pub unsafe extern "C-unwind" fn decode_integer_message(state: LuaState, m: *mut Message) -> i32 {
    unsafe {
        let body = (*m).take_body();
        match body {
            MessageBody::ISize(_, v) => {
                moon_lua::laux::lua_push(state, v);
                1
            }
            other => {
                let ptype = (*m).ptype();
                (*m).data = other;
                lua_push_error(
                    state,
                    &format!(
                        "expected ISize message body for ptype {}, got {}",
                        ptype,
                        body_discriminant(&(*m).data)
                    ),
                )
            }
        }
    }
}

pub unsafe extern "C-unwind" fn decode_buffer_as_string_message(
    state: LuaState,
    m: *mut Message,
) -> i32 {
    unsafe {
        // Borrow (don't take): `lua_push` can longjmp on Lua OOM, which would skip
        // the `Drop` of a taken `Box<Buffer>` and leak it.
        match borrow_buffer(m) {
            Ok(buf) => {
                moon_lua::laux::lua_push(state, buf.as_slice());
                1
            }
            Err(e) => lua_push_error(state, &e),
        }
    }
}
