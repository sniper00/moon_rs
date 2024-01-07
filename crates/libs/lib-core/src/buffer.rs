#[allow(dead_code)]
#[derive(Debug)]
pub struct Buffer {
    data: Vec<u8>,
    rpos: usize,
    head_reserved: usize,
}

const DEFAULT_HEAD_RESERVE: usize = 16;
const DEFAULT_RESERVE: usize = 128 - DEFAULT_HEAD_RESERVE;

#[allow(dead_code)]
impl Buffer {
    pub fn new() -> Buffer {
        let mut raw = Vec::<u8>::with_capacity(DEFAULT_RESERVE + DEFAULT_HEAD_RESERVE);
        raw.resize(DEFAULT_HEAD_RESERVE, 0);
        Buffer {
            data: raw,
            rpos: DEFAULT_HEAD_RESERVE,
            head_reserved: DEFAULT_HEAD_RESERVE,
        }
    }

    pub fn with_reserve(reserve: usize) -> Buffer {
        let mut raw = Vec::<u8>::with_capacity(reserve + DEFAULT_HEAD_RESERVE);
        raw.resize(DEFAULT_HEAD_RESERVE, 0);
        Buffer {
            data: raw,
            rpos: DEFAULT_HEAD_RESERVE,
            head_reserved: DEFAULT_HEAD_RESERVE,
        }
    }

    pub fn with_head_reserve(reserve: usize, head_reserve: usize) -> Buffer {
        let mut raw = Vec::<u8>::with_capacity(reserve + head_reserve);
        raw.resize(head_reserve, 0);
        Buffer {
            data: raw,
            rpos: head_reserve,
            head_reserved: head_reserve,
        }
    }

    pub fn write_slice(&mut self, data: &[u8]) {
        self.prepare(data.len());
        self.data.extend_from_slice(data);
    }

    pub fn write(&mut self, c: u8) {
        self.data.push(c);
    }

    pub fn unsafe_write(&mut self, c: u8) {
        unsafe {
            let len = self.data.len() + 1;
            self.data.set_len(len);
            *self.data.get_unchecked_mut(len - 1) = c;
        }
    }

    pub fn write_front(&mut self, data: &[u8]) -> bool {
        let len: usize = data.len();
        if len > self.rpos {
            return false;
        }
        self.rpos -= len;
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.data.as_mut_ptr().add(self.rpos),
                len,
            );
        }
        true
    }

    pub fn write_chars<T>(&mut self, data: T)
    where
        T: ToString,
    {
        let s = data.to_string();
        self.write_slice(s.as_bytes());
    }

    pub fn write_str(&mut self, data: &str) {
        self.write_slice(data.as_bytes());
    }

    pub fn read(&mut self, count: usize) -> Option<Vec<u8>> {
        if self.data.len() < self.rpos + count {
            return None;
        }
        let mut v = Vec::with_capacity(count);
        unsafe {
            std::ptr::copy_nonoverlapping(self.data.as_ptr().add(self.rpos), v.as_mut_ptr(), count);
            v.set_len(count);
        }
        self.rpos += count;
        Some(v)
    }

    pub fn consume(&mut self, count: usize) {
        if self.data.len() < self.rpos + count {
            return;
        }
        self.rpos += count;
    }

    pub fn seek(&mut self, pos: usize) -> bool {
        if self.data.len() < pos {
            return false;
        }
        self.rpos = pos;
        true
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.rpos = self.head_reserved;
        if self.head_reserved > 0 {
            self.data.resize(self.head_reserved, 0);
        }
    }

    pub fn len(&self) -> usize {
        self.data.len() - self.rpos
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == self.rpos
    }

    pub fn prepare(&mut self, size: usize) -> std::option::Option<(*mut u8, usize)> {
        let tail_free_space = self.data.capacity() - self.data.len();
        if tail_free_space >= size {
            return None;
        }

        let count = self.data.len() - self.rpos;
        if tail_free_space + self.rpos >= size + self.head_reserved {
            unsafe {
                if count != 0 {
                    //println!("move data");
                    std::ptr::copy(
                        self.data.as_ptr().add(self.rpos),
                        self.data.as_mut_ptr().add(self.head_reserved),
                        count,
                    );
                }
                self.rpos = self.head_reserved;
                self.data.set_len(self.rpos + count);
            }
        } else {
            //println!("copy data");
            let required_size = self.data.len() + size;
            let mut new_vec = Vec::<u8>::with_capacity(required_size);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.data.as_ptr(),
                    new_vec.as_mut_ptr(),
                    self.data.len(),
                );
                new_vec.set_len(self.data.len());
                self.data = new_vec;
            }
        }

        unsafe { Some((self.data.as_mut_ptr().add(self.data.len()), size)) }
    }

    pub fn commit(&mut self, size: usize) {
        let len = self.data.len() + size;
        assert!(
            len <= self.data.capacity(),
            "commit size is larger than prepare size",
        );
        unsafe {
            self.data.set_len(len);
        }
    }

    pub fn revert(&mut self, size: usize) {
        assert!(
            self.data.len() >= self.rpos + size,
            "revert size is larger than readable size",
        );

        unsafe {
            self.data.set_len(self.data.len() - size);
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.data[self.rpos..]
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.data.as_ptr().add(self.rpos) }
    }

    pub fn read_i16(&mut self, pos: usize, le: bool) -> i16 {
        let mut v = 0i16;
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.data.as_ptr().add(self.rpos + pos),
                &mut v as *mut i16 as *mut u8,
                2,
            );
        }
        if le {
            v.to_le()
        } else {
            v.to_be()
        }
    }

    pub fn read_u16(&mut self, pos: usize, le: bool) -> u16 {
        let mut v = 0u16;
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.data.as_ptr().add(self.rpos + pos),
                &mut v as *mut u16 as *mut u8,
                2,
            );
        }
        if le {
            v.to_le()
        } else {
            v.to_be()
        }
    }

    pub fn read_i32(&mut self, pos: usize, le: bool) -> i32 {
        let mut v = 0i32;
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.data.as_ptr().add(self.rpos + pos),
                &mut v as *mut i32 as *mut u8,
                4,
            );
        }
        if le {
            v.to_le()
        } else {
            v.to_be()
        }
    }

    pub fn read_u32(&mut self, pos: usize, le: bool) -> u32 {
        let mut v = 0u32;
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.data.as_ptr().add(self.rpos + pos),
                &mut v as *mut u32 as *mut u8,
                4,
            );
        }
        if le {
            v.to_le()
        } else {
            v.to_be()
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    pub fn as_pointer(&mut self) -> *mut Buffer {
        self as *mut Buffer
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(v: Vec<u8>) -> Self {
        Buffer {
            data: v,
            rpos: 0,
            head_reserved: 0,
        }
    }
}

impl From<&[u8]> for Buffer {
    fn from(v: &[u8]) -> Self {
        Buffer {
            data: v.to_vec(),
            rpos: 0,
            head_reserved: 0,
        }
    }
}

impl From<&str> for Buffer {
    fn from(v: &str) -> Self {
        Buffer {
            data: v.as_bytes().to_vec(),
            rpos: 0,
            head_reserved: 0,
        }
    }
}

impl From<String> for Buffer {
    fn from(v: String) -> Self {
        Buffer {
            data: v.into_bytes(),
            rpos: 0,
            head_reserved: 0,
        }
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Buffer::new()
    }
}
