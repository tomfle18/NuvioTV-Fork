use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Cancelled,
    Invalid,
    Unsupported,
    Changed,
    Protocol,
    Http(u16),
    Timeout,
    Network,
    Budget,
    NoSlot,
    Internal,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Deliberately excludes URL, request headers and underlying TLS/HTTP error text.
        write!(f, "rust-streaming:{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
