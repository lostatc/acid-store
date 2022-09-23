use std::error::Error as StdError;
use std::fmt;
use std::ops::Deref;
use std::result;

/// An error type for an error that occurred in a data store.
#[derive(Debug)]
pub struct Error {
    inner: anyhow::Error,
}

impl Error {
    /// Construct a new `Error` that wraps the given `error`.
    pub fn new<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            inner: anyhow::Error::new(error),
        }
    }
}

impl<E> From<E> for Error
where
    E: StdError + Send + Sync + 'static,
{
    fn from(error: E) -> Self {
        Self::new(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl AsRef<dyn StdError + Send + Sync + 'static> for Error {
    fn as_ref(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self.inner.as_ref()
    }
}

impl Deref for Error {
    type Target = dyn StdError + Send + Sync + 'static;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

/// A result type for an error that occurred in a data store.
pub type Result<T> = result::Result<T, Error>;