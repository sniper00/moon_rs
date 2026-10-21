//! A growable, seekable byte buffer shared across moon_rs crates.
//!
//! Dependency-free (std only) so it can be linked into the main runtime as well
//! as standalone Lua C extension cdylibs without pulling in any extra crates.

use std::{fmt, fmt::Write as _, mem::MaybeUninit};

#[allow(dead_code)]
#[derive(Debug)]
pub struct Buffer {
    rpos: usize,
    data: Vec<u8>,
}

pub const DEFAULT_RESERVE: usize = 128;
pub const BUFFER_HEAD_RESERVE: usize = 16;

#[allow(dead_code)]
impl Buffer {
    pub fn new() -> Buffer {
        Buffer {
            data: Vec::<u8>::with_capacity(DEFAULT_RESERVE),
            rpos: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Buffer {
        Buffer {
            data: Vec::<u8>::with_capacity(capacity),
            rpos: 0,
        }
    }

    pub fn from_slice(data: &[u8]) -> Buffer {
        let mut raw = Vec::<u8>::with_capacity(data.len());
        raw.extend_from_slice(data);
        Buffer { data: raw, rpos: 0 }
    }

    pub fn write_slice(&mut self, data: &[u8]) {
        self.prepare(data.len());
        self.data.extend_from_slice(data);
    }

    pub fn write(&mut self, c: u8) {
        self.data.push(c);
    }

    /// Append a byte without checking or growing capacity.
    ///
    /// # Safety
    /// There must be at least one byte of spare capacity.
    pub unsafe fn unsafe_write(&mut self, c: u8) {
        debug_assert!(self.data.len() < self.data.capacity());
        unsafe {
            let len = self.data.len();
            self.data.as_mut_ptr().add(len).write(c);
            self.data.set_len(len + 1);
        }
    }

    #[must_use]
    pub fn write_front(&mut self, data: &[u8]) -> bool {
        let len: usize = data.len();
        if len > self.rpos {
            return false;
        }
        self.rpos -= len;
        self.data[self.rpos..self.rpos + len].copy_from_slice(data);
        true
    }

    pub fn write_front_byte(&mut self, c: u8) -> bool {
        if self.rpos == 0 {
            return false;
        }
        self.rpos -= 1;
        self.data[self.rpos] = c;
        true
    }

    pub fn write_chars<T>(&mut self, data: T) -> fmt::Result
    where
        T: fmt::Display,
    {
        write!(self, "{data}")
    }

    pub fn write_str(&mut self, data: &str) {
        self.write_slice(data.as_bytes());
    }

    pub fn read(&mut self, count: usize) -> Option<Vec<u8>> {
        if count > self.len() {
            return None;
        }
        let v = self.as_slice()[..count].to_vec();
        self.rpos += count;
        Some(v)
    }

    pub fn consume(&mut self, count: usize) {
        if count > self.len() {
            return;
        }
        self.rpos += count;
    }

    pub fn seek(&mut self, pos: isize) -> bool {
        if pos < 0 {
            if self.rpos < pos.unsigned_abs() {
                return false;
            }
            self.rpos -= pos.unsigned_abs();
            return true;
        }

        if pos as usize > self.len() {
            return false;
        }
        self.rpos += pos as usize;
        true
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.rpos = 0;
    }

    pub fn len(&self) -> usize {
        self.data.len() - self.rpos
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == self.rpos
    }

    /// Reserve `size` bytes of contiguous spare capacity at the tail (compacting
    /// already-consumed front bytes and growing as needed) and return a raw
    /// pointer to the start of that region.
    ///
    /// The region is **uninitialized**: the caller must write the bytes it needs
    /// and then call [`commit`](Self::commit) to extend the logical length. A raw
    /// pointer (rather than `&mut [u8]`) is returned because these bytes are not
    /// initialized. Prefer [`spare_capacity_mut`](Self::spare_capacity_mut) when
    /// passing the region to APIs that accept `MaybeUninit<u8>`.
    pub fn prepare(&mut self, size: usize) -> *mut u8 {
        let tail_free_space = self.data.capacity() - self.data.len();
        if tail_free_space < size {
            if self.rpos != 0 {
                let count = self.len();
                self.data.copy_within(self.rpos.., 0);
                self.data.truncate(count);
                self.rpos = 0;
            }
            self.data.reserve(size);
        }

        unsafe { self.data.as_mut_ptr().add(self.data.len()) }
    }

    /// Reserve and borrow exactly `size` bytes of spare capacity.
    pub fn spare_capacity_mut(&mut self, size: usize) -> &mut [MaybeUninit<u8>] {
        self.prepare(size);
        &mut self.data.spare_capacity_mut()[..size]
    }

    /// Extend the written length without copying or initializing bytes.
    /// Returns false without changing the buffer if capacity is insufficient.
    ///
    /// # Safety
    /// If `size` fits, all `size` bytes immediately after the current written
    /// length must already be initialized. No reference to that region may
    /// remain in use after committing it.
    #[must_use]
    pub unsafe fn commit(&mut self, size: usize) -> bool {
        if size > self.data.capacity() - self.data.len() {
            return false;
        }

        unsafe {
            self.data.set_len(self.data.len() + size);
        }

        true
    }

    pub fn revert(&mut self, size: usize) {
        assert!(
            size <= self.len(),
            "revert size is larger than readable size",
        );

        self.data.truncate(self.data.len() - size);
    }

    pub fn data(&self) -> &[u8] {
        &self.data[self.rpos..]
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.as_slice().as_ptr()
    }

    pub fn read_u8(&self, pos: usize) -> u8 {
        self.as_slice()[pos]
    }

    pub fn read_i16(&self, pos: usize, le: bool) -> i16 {
        let bytes = self.as_slice()[pos..][..2].try_into().unwrap();
        if le {
            i16::from_le_bytes(bytes)
        } else {
            i16::from_be_bytes(bytes)
        }
    }

    pub fn read_u16(&self, pos: usize, le: bool) -> u16 {
        let bytes = self.as_slice()[pos..][..2].try_into().unwrap();
        if le {
            u16::from_le_bytes(bytes)
        } else {
            u16::from_be_bytes(bytes)
        }
    }

    pub fn read_i32(&self, pos: usize, le: bool) -> i32 {
        let bytes = self.as_slice()[pos..][..4].try_into().unwrap();
        if le {
            i32::from_le_bytes(bytes)
        } else {
            i32::from_be_bytes(bytes)
        }
    }

    pub fn read_u32(&self, pos: usize, le: bool) -> u32 {
        let bytes = self.as_slice()[pos..][..4].try_into().unwrap();
        if le {
            u32::from_le_bytes(bytes)
        } else {
            u32::from_be_bytes(bytes)
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.rpos..]
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data[self.rpos..]
    }

    /// Access storage including the consumed prefix.
    ///
    /// # Safety
    /// The vector's length must remain at least `read_pos()`, including if a
    /// mutation panics. Prefer append-only writes through this reference.
    pub unsafe fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    pub fn as_vec(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn as_pointer(&mut self) -> *mut Buffer {
        self as *mut Buffer
    }

    pub fn as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.as_slice())
    }

    /// Shift `count` bytes within the underlying storage from absolute offset `src`
    /// to absolute offset `dst`. Handles overlapping regions via `memmove`.
    /// Both `src..src+count` must be within the written data, and
    /// `dst..dst+count` must be within buffer capacity.
    pub fn shift_data(&mut self, src: usize, count: usize, dst: usize) {
        assert!(
            src <= self.data.len() && count <= self.data.len() - src,
            "shift_data: src({}) + count({}) > data.len({})",
            src,
            count,
            self.data.len()
        );
        assert!(
            dst <= self.data.capacity() && count <= self.data.capacity() - dst,
            "shift_data: dst({}) + count({}) > capacity({})",
            dst,
            count,
            self.data.capacity()
        );
        if src == dst || count == 0 {
            return;
        }
        unsafe {
            let ptr = self.data.as_mut_ptr();
            std::ptr::copy(ptr.add(src), ptr.add(dst), count);
        }
    }

    /// Returns the current write position (absolute offset into the underlying Vec).
    pub fn write_pos(&self) -> usize {
        self.data.len()
    }

    /// Returns the read position (absolute offset into the underlying Vec).
    pub fn read_pos(&self) -> usize {
        self.rpos
    }

    /// Get a mutable reference to the underlying data at an absolute offset.
    /// Panics if `offset + len` exceeds the written data length.
    pub fn data_mut_at(&mut self, offset: usize, len: usize) -> &mut [u8] {
        &mut self.data[offset..][..len]
    }

    /// Transfer the allocation and its readable offset without moving the
    /// remaining bytes. Callers that can represent a slice of the allocation
    /// can avoid the compaction performed by `into_vec`.
    pub fn into_vec_with_offset(self) -> (Vec<u8>, usize) {
        (self.data, self.rpos)
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(v: Vec<u8>) -> Self {
        Buffer { data: v, rpos: 0 }
    }
}

impl From<&[u8]> for Buffer {
    fn from(v: &[u8]) -> Self {
        Buffer {
            data: v.to_vec(),
            rpos: 0,
        }
    }
}

impl From<&str> for Buffer {
    fn from(v: &str) -> Self {
        Buffer {
            data: v.as_bytes().to_vec(),
            rpos: 0,
        }
    }
}

impl From<String> for Buffer {
    fn from(v: String) -> Self {
        Buffer {
            data: v.into_bytes(),
            rpos: 0,
        }
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Buffer::new()
    }
}

impl fmt::Display for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(self.as_slice()))
    }
}

impl fmt::Write for Buffer {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.write_slice(text.as_bytes());
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/unit/buffer.rs"]
mod tests;
