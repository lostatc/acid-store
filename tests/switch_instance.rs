#![cfg(all(feature = "encryption", feature = "compression"))]

use std::io::Write;

use acid_store::repo::key::KeyRepo;
use acid_store::repo::{Commit, SwitchInstance, DEFAULT_INSTANCE};
use acid_store::uuid::Uuid;

use common::*;

mod common;

#[rstest]
fn switching_instance_does_not_roll_back(repo_object: RepoObject) -> anyhow::Result<()> {
    let RepoObject {
        repo,
        mut object,
        key,
    } = repo_object;

    object.write_all(b"test data")?;
    object.flush()?;
    drop(object);

    let repo: KeyRepo<String> = repo.switch_instance(Uuid::new_v4().into())?;
    let repo: KeyRepo<String> = repo.switch_instance(DEFAULT_INSTANCE)?;

    assert_that!(repo.contains(&key)).is_true();
    assert_that!(repo.object(&key)).is_some();

    Ok(())
}

#[rstest]
fn switching_instance_does_not_commit(repo_object: RepoObject) -> anyhow::Result<()> {
    let RepoObject {
        repo,
        mut object,
        key,
    } = repo_object;

    object.write_all(b"test data")?;
    object.flush()?;
    drop(object);

    let repo: KeyRepo<String> = repo.switch_instance(Uuid::new_v4().into())?;
    let mut repo: KeyRepo<String> = repo.switch_instance(DEFAULT_INSTANCE)?;
    repo.rollback()?;

    assert_that!(repo.contains(&key)).is_false();
    assert_that!(repo.object(&key)).is_none();

    Ok(())
}
