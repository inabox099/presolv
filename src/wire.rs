pub const HEADER_LEN: usize = 12;

pub fn read_id(b: &[u8]) -> Option<u16> {
    if b.len() < 2 {
        return None;
    }
    Some(u16::from_be_bytes([b[0], b[1]]))
}

pub fn write_id(b: &mut [u8], id: u16) -> bool {
    if b.len() < 2 {
        return false;
    }
    b[..2].copy_from_slice(&id.to_be_bytes());
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_id_is_big_endian() {
        assert_eq!(read_id(&[0x12, 0x34, 0xff]), Some(0x1234));
    }

    #[test]
    fn read_id_needs_two_bytes() {
        assert_eq!(read_id(&[0x12]), None);
        assert_eq!(read_id(&[]), None);
    }

    #[test]
    fn write_id_overwrites_first_two_bytes_only() {
        let mut b = vec![0, 0, 0xaa, 0xbb];
        assert!(write_id(&mut b, 0xbeef));
        assert_eq!(b, vec![0xbe, 0xef, 0xaa, 0xbb]);
        assert!(!write_id(&mut [0u8; 1], 1));
    }
}
