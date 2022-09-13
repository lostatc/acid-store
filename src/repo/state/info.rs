use serde::{Deserialize, Serialize};

use crate::repo::key::KeyRepo;
use crate::repo::{InstanceId, RepoId, Restore, RestoreSavepoint};

id_table! {
    /// An ID that uniquely identifies an `ObjectKey` in a `StateRepo`.
    KeyId

    /// A table for allocating `KeyId` values.
    KeyIdTable
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum RepoKey {
    Object(KeyId),
    State,
    IdTable,
    Stage,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepoState<State> {
    pub state: State,
    pub id_table: KeyIdTable,
}

#[derive(Debug, Clone)]
pub struct StateRestore<State> {
    pub state: RepoState<State>,
    pub restore: <KeyRepo<RepoKey> as RestoreSavepoint>::Restore,
}

impl<State: Clone> Restore for StateRestore<State> {
    fn is_valid(&self) -> bool {
        self.restore.is_valid()
    }

    fn instance(&self) -> InstanceId {
        self.restore.instance()
    }
}

/// An opaque key which can be used to access an object in a [`StateRepo`].
///
/// [`StateRepo`]: crate::repo::state::StateRepo
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct ObjectKey {
    pub(super) repo_id: RepoId,
    pub(super) instance_id: InstanceId,
    pub(super) key_id: KeyId,
}
