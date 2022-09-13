use crate::store::DataStore;

/// A value which can be used to open a `DataStore`.
pub trait OpenStore {
    /// The type of `DataStore` which this value can be used to open.
    type Store: DataStore + 'static;

    /// Open or create a data store of type `Store`.
    ///
    /// This opens the data store, creating it if it does not already exist.
    ///
    /// # Errors
    /// - `Error::UnsupportedStore`: The data store is an unsupported format. This can happen if
    /// the serialized data format changed or if the storage represented by this value does not
    /// contain a valid data store.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    fn open(&self) -> crate::Result<Self::Store>;
}
