use std::{future::poll_fn, io, io::IoSlice, pin::Pin};

use moon_lua_sys::buffer::Buffer;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

/// Read directly into spare capacity, publishing bytes only after a full read.
pub(crate) async fn read_buffer_exact<R: AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &mut Buffer,
    size: usize,
) -> io::Result<()> {
    let mut out = ReadBuf::uninit(buffer.spare_capacity_mut(size));
    while out.remaining() != 0 {
        let before = out.filled().len();
        poll_fn(|cx| Pin::new(&mut *reader).poll_read(cx, &mut out)).await?;
        if out.filled().len() == before {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "early eof"));
        }
    }
    // SAFETY: ReadBuf filled the entire reserved region; its borrow ends here.
    let committed = unsafe { buffer.commit(size) };
    debug_assert!(committed);
    Ok(())
}

/// Advance the existing iovecs after partial writes without allocating.
pub(crate) async fn write_all_vectored<W: AsyncWrite + Unpin>(
    writer: &mut W,
    mut slices: &mut [IoSlice<'_>],
    write_zero_message: &'static str,
) -> io::Result<()> {
    IoSlice::advance_slices(&mut slices, 0);
    while !slices.is_empty() {
        let written = writer.write_vectored(slices).await?;
        if written == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, write_zero_message));
        }
        IoSlice::advance_slices(&mut slices, written);
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/net_io.rs"]
mod tests;
