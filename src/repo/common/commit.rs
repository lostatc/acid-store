use static_assertions::assert_obj_safe;

/// A repository which supports committing and rolling back changes.
pub trait Commit {
    /// Commit changes which have been made to the repository.
    ///
    /// No changes are saved persistently until this method is called.
    ///
    /// If this method returns `Ok`, changes have been committed. If this method returns `Err`,
    /// changes have not been committed.
    ///
    /// If changes are committed, this method invalidates all savepoints which are associated with
    /// this repository.
    ///
    /// To reclaim space from deleted objects in the backing data store, you must call [`clean`]
    /// after changes are committed.
    ///
    /// This method commits changes for all instances of the repository.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`clean`]: crate::repo::Commit::clean
    fn commit(&mut self) -> crate::Result<()>;

    /// Roll back all changes made since the last commit.
    ///
    /// Uncommitted changes in a repository are automatically rolled back when the repository is
    /// dropped. This method can be used to manually roll back changes without dropping and
    /// re-opening the repository.
    ///
    /// If this method returns `Ok`, changes have been rolled back. If this method returns `Err`,
    /// the repository is unchanged.
    ///
    /// This method rolls back changes for all instances of the repository.
    ///
    /// Rolling back changes invalidates all [`Object`] and [`ReadOnlyObject`] instances associated
    /// with the repository.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`Object`]: crate::repo::Object
    /// [`ReadOnlyObject`]: crate::repo::ReadOnlyObject
    fn rollback(&mut self) -> crate::Result<()>;

    /// Clean up the repository to reclaim space in the backing data store.
    ///
    /// When data in a repository is deleted, the space is not reclaimed in the backing data store
    /// until those changes are committed and this method is called.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    fn clean(&mut self) -> crate::Result<()>;
}

assert_obj_safe!(Commit);
