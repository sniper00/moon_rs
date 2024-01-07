#[cfg(test)]
mod tests {
    use std::vec;
    use moon_rs::moon_core::buffer::Buffer;

    #[test]
    fn test_buffer_1() {
        let mut buf = Buffer::with_head_reserve(8, 4);
        buf.write_str("1234567");
        assert_eq!(buf.write_front("bbbb".as_bytes()), true);
    
        let r = buf.read(8);
        log::info!("{}", String::from_utf8_lossy(r.unwrap().as_ref()));
    
        buf.write_str("abcd");
        assert!(buf.read(4).unwrap() == "567a".as_bytes());
        assert!(buf.len() == 3);
    }
    #[test]
    fn test_buffer_2(){
        let mut buf = Buffer::with_head_reserve(128, 0);
        assert!(buf.len() == 0);
        let n:i32 = 0;
        buf.write_slice(n.to_le_bytes().as_ref());
        assert_eq!(buf.len(), 4);
        assert!(buf.read(4).unwrap() == n.to_le_bytes().as_ref());
        assert!(buf.len() == 0);
        assert!(buf.read(4).is_none());
    }

    #[test]
    fn test_buffer_3(){
        let mut buf = Buffer::with_head_reserve(32, 4);
        assert!(buf.len() == 0);

        for _ in 0..100 {
            buf.write_slice(vec![0;17].as_ref());
            assert!(buf.read(17).is_some());
        }

        let n:i32 = 117;
        assert_eq!(buf.write_front(n.to_le_bytes().as_ref()), true);
        let mut bf:[u8;4] = [0;4];
        bf.copy_from_slice(buf.read(4).unwrap().as_slice());
        assert_eq!(i32::from_le_bytes(bf) , 117);
    }
}
