use std::error::Error as StdError;
use std::fmt;
use std::ops::Deref;
use std::result;

use static_assertions::assert_impl_all;

/// An error type for an error that occurred in a data store.
///
/// This wraps a dynamic error type.
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
    /// If the argument implements [`std::error::Error`], use [`new`] instead. If an `Error` is
    /// constructed this way, it cannot be downcast back to its original value.
    pub fn msg<M>(message: M) -> Self
    where
        M: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self {
            inner: anyhow::Error::msg(message),
        }
    }

    /// Return `true` if `E` is the type held by this error object.
    pub fn is<E>(&self) -> bool
    where
        E: StdError + Send + Sync + 'static,
    {
        self.inner.is::<E>()
    }

    /// Attempt to downcast this error object to a concrete type.
    pub fn downcast<E>(self) -> Result<E>
    where
        E: StdError + Send + Sync + 'static,
    {
        self.inner
            .downcast::<E>()
            .map_err(|error| Self { inner: error })
    }

    /// Attempt to downcast this error object to a concrete type by reference.
    pub fn downcast_ref<E>(&self) -> Option<&E>
    where
        E: StdError + Send + Sync + 'static,
    {
        self.inner.downcast_ref()
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

impl From<Error> for Box<dyn StdError + 'static> {
    fn from(error: Error) -> Self {
        error.into()
    }
}

impl From<Error> for Box<dyn StdError + Send + 'static> {
    fn from(error: Error) -> Self {
        error.into()
    }
}

impl From<Error> for Box<dyn StdError + Send + Sync + 'static> {
    fn from(error: Error) -> Self {
        error.into()
    }
}

assert_impl_all!(Error: Send, Sync);

/// A result type for an error that occurred in a data store.
pub type Result<T> = result::Result<T, Error>;
