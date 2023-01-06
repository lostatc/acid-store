use std::error::Error as StdError;
use std::fmt;
use std::ops::Deref;
use std::result;

/// An error that occurs in a [`DataStore`].
///
/// This wraps a dynamic error type.
///
/// [`DataStore`]: crate::store::DataStore
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

    /// Construct a new `Error` from a printable error message.
    ///
    /// If the argument implements [`std::error::Error`], use [`new`] instead.
    ///
    /// [`new`]: crate::store::Error::new
    pub fn msg<M>(message: M) -> Self
    where
        M: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self {
            inner: anyhow::Error::msg(message),
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

impl From<Error> for Box<dyn StdError + Send + Sync + 'static> {
    fn from(error: Error) -> Self {
        error.into()
    }
}

/// A result type for [`Error`].
pub type Result<T> = result::Result<T, Error>;
