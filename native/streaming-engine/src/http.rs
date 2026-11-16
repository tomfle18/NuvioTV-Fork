use crate::error::{Error, Result};
use reqwest::header::{
    HeaderMap, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, LAST_MODIFIED,
};
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Representation {
    pub total: Option<u64>,
    pub etag: Option<String>,
    pub modified: Option<String>,
}
impl Representation {
    pub fn validator(&self) -> Option<&str> {
        self.etag
            .as_deref()
            .filter(|s| !s.starts_with("W/"))
            .or(self.modified.as_deref())
    }
    pub fn compatible(&self, other: &Self) -> bool {
        self.etag == other.etag
            && self.modified == other.modified
            && (self.total.is_none() || other.total.is_none() || self.total == other.total)
    }
}

pub fn one<'a>(headers: &'a HeaderMap, key: &str) -> Result<Option<&'a str>> {
    let mut all = headers.get_all(key).iter();
    let first = all.next();
    if all.next().is_some() {
        return Err(Error::Protocol);
    }
    first
        .map(|s| s.to_str().map_err(|_| Error::Protocol))
        .transpose()
}
pub use crate::range::{content_range, number};

pub struct Validated {
    pub identity: Representation,
    pub length: Option<u64>,
    pub ranged: bool,
}
pub fn validate(status: u16, headers: &HeaderMap, start: u64, end: u64) -> Result<Validated> {
    // Android owns the mutable cookie jar. A source can introduce cookies on its
    // first response even when qualification saw an empty jar. Let Kotlin repeat
    // that open before publishing bytes rather than silently dropping Set-Cookie.
    if matches!(status, 200 | 206) && headers.contains_key("set-cookie") {
        return Err(Error::Unsupported);
    }
    if one(headers, CONTENT_ENCODING.as_str())?.is_some_and(|s| !s.eq_ignore_ascii_case("identity"))
    {
        return Err(Error::Protocol);
    }
    let length = one(headers, CONTENT_LENGTH.as_str())?
        .map(number)
        .transpose()?;
    if length.is_some_and(|n| n > i64::MAX as u64) {
        return Err(Error::Protocol);
    }
    let mut identity = Representation {
        total: None,
        etag: one(headers, ETAG.as_str())?.map(str::to_owned),
        modified: one(headers, LAST_MODIFIED.as_str())?.map(str::to_owned),
    };
    match status {
        206 => {
            let (a, b, total) =
                content_range(one(headers, CONTENT_RANGE.as_str())?.ok_or(Error::Protocol)?)?;
            if a != start
                || b > end
                || (b < end && total.is_some() && total != Some(b + 1))
                || length.is_some_and(|n| n != b - a + 1)
            {
                return Err(Error::Protocol);
            }
            identity.total = total;
            Ok(Validated {
                identity,
                length: Some(b - a + 1),
                ranged: true,
            })
        }
        200 if start == 0 => {
            if one(headers, CONTENT_RANGE.as_str())?.is_some() {
                return Err(Error::Protocol);
            }
            identity.total = length;
            Ok(Validated {
                identity,
                length,
                ranged: false,
            })
        }
        200 => Err(Error::Unsupported),
        416 => Err(Error::Http(416)), // caller must prove EOF using bytes */N and its known identity
        s => Err(Error::Http(s)),
    }
}
pub fn retry_after(s: Option<&str>) -> Duration {
    let seconds = s
        .and_then(|s| {
            s.parse::<u64>().ok().or_else(|| {
                httpdate::parse_http_date(s)
                    .ok()?
                    .duration_since(SystemTime::now())
                    .ok()
                    .map(|d| d.as_secs())
            })
        })
        .unwrap_or(1);
    // Do not retry sooner than a long Retry-After: surface it to the app instead of clamping it.
    Duration::from_secs(seconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_ranges_and_large_offsets() {
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
            assert!(content_range(s).is_err(), "{s}");
        }
    }
    #[test]
    fn response_contract() {
        let mut h = HeaderMap::new();
        h.insert(CONTENT_RANGE, "bytes 5-9/10".parse().unwrap());
        h.insert(CONTENT_LENGTH, "5".parse().unwrap());
        assert!(validate(206, &h, 5, 20).is_ok());
        assert!(validate(206, &h, 6, 20).is_err());
        assert!(validate(200, &HeaderMap::new(), 5, 20).is_err());
        h.append(CONTENT_RANGE, "bytes 5-9/10".parse().unwrap());
        assert!(validate(206, &h, 5, 20).is_err());
        let mut cookies = HeaderMap::new();
        cookies.insert("set-cookie", "session=fixture; HttpOnly".parse().unwrap());
        assert_eq!(
            validate(200, &cookies, 0, 20).err(),
            Some(Error::Unsupported)
        );
    }
    #[test]
    fn encoding_and_changed_identity() {
        let mut h = HeaderMap::new();
        h.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        assert_eq!(validate(200, &h, 0, 5).err(), Some(Error::Protocol));
        let a = Representation {
            total: Some(3),
            etag: Some("\"a\"".into()),
            modified: None,
        };
        let mut b = a.clone();
        b.etag = Some("\"b\"".into());
        assert!(!a.compatible(&b));
    }
}
