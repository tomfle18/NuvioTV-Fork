use crate::error::{Error, Result};
pub fn number(s: &str) -> Result<u64> {
    if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
        return Err(Error::Protocol);
    }
    s.parse().map_err(|_| Error::Protocol)
}
pub fn content_range(s: &str) -> Result<(u64, u64, Option<u64>)> {
    let s = s.strip_prefix("bytes ").ok_or(Error::Protocol)?;
    let (interval, total) = s.split_once('/').ok_or(Error::Protocol)?;
    let (a, b) = interval.split_once('-').ok_or(Error::Protocol)?;
    let (a, b) = (number(a)?, number(b)?);
    let total = if total == "*" {
        None
    } else {
        Some(number(total)?)
    };
    if b < a || b > i64::MAX as u64 || total.is_some_and(|t| b >= t || t > i64::MAX as u64) {
        return Err(Error::Protocol);
    }
    Ok((a, b, total))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn large_offsets_and_malformed_ranges() {
        assert_eq!(
            content_range("bytes 4294967297-4294967300/5000000000").unwrap(),
            (4294967297, 4294967300, Some(5000000000))
        );
        for s in [
            "bytes 0-2/2",
            "bytes 3-2/9",
            "bytes +1-2/9",
            "bytes 0-18446744073709551615/*",
            "bytes 0-2/4/5",
            "bytes 0-2/-1",
        ] {
            assert!(content_range(s).is_err());
        }
    }
    #[test]
    #[cfg(not(miri))]
    fn deterministic_parser_fuzz_smoke() {
        let alphabet = b"bytes */-0123456789\r\n\0:+";
        let mut random = 71u64;
        for _ in 0..100_000 {
            let mut bytes = [0u8; 80];
            for byte in &mut bytes {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                *byte = alphabet[(random >> 32) as usize % alphabet.len()];
            }
            if let Ok((start, end, total)) = content_range(std::str::from_utf8(&bytes).unwrap()) {
                assert!(start <= end);
                assert!(total.is_none_or(|n| n > end));
            }
        }
    }
}
