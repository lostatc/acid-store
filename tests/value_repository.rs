#![cfg(all(
    feature = "repo-value",
    feature = "encryption",
    feature = "compression"
))]

use std::collections::HashSet;

use acid_store::repo::value::ValueRepo;
use acid_store::repo::{Commit, SwitchInstance, DEFAULT_INSTANCE};
use acid_store::uuid::Uuid;
use common::*;

mod common;

type TestType = (bool, i32);

const TEST_VALUE: TestType = (true, 42);

#[rstest]
fn switching_instance_does_not_roll_back(mut repo: ValueRepo<String>) -> anyhow::Result<()> {
    repo.insert("test".to_string(), &TEST_VALUE)?;

    let repo: ValueRepo<String> = repo.switch_instance(Uuid::new_v4().into())?;
    let repo: ValueRepo<String> = repo.switch_instance(DEFAULT_INSTANCE)?;

    assert_that!(repo.contains("test")).is_true();
    assert_that!(repo.get::<_, TestType>("test")).is_ok();

    Ok(())
}

#[rstest]
fn switching_instance_does_not_commit(mut repo: ValueRepo<String>) -> anyhow::Result<()> {
    repo.insert("test".to_string(), &TEST_VALUE)?;

    let repo: ValueRepo<String> = repo.switch_instance(Uuid::new_v4().into())?;
    let mut repo: ValueRepo<String> = repo.switch_instance(DEFAULT_INSTANCE)?;
    repo.rollback()?;

    assert_that!(repo.contains("test")).is_false();
    assert_that!(repo.get::<_, TestType>("test")).is_err_variant(acid_store::Error::NotFound);

    Ok(())
}

#[rstest]
fn insert_value(mut repo: ValueRepo<String>) {
    assert_that!(repo.insert("test".into(), &TEST_VALUE)).is_ok();
    assert_that!(repo.get("test")).is_ok_containing(TEST_VALUE);
}

#[rstest]
fn remove_value(mut repo: ValueRepo<String>) {
    assert_that!(repo.remove("Key")).is_false();
    assert_that!(repo.contains("Key")).is_false();

    assert_that!(repo.insert("Key".into(), &TEST_VALUE)).is_ok();

    assert_that!(repo.contains("Key")).is_true();
    assert_that!(repo.remove("Key")).is_true();
    assert_that!(repo.contains("Key")).is_false();
}

#[rstest]
fn deserializing_value_to_wrong_type_errs(mut repo: ValueRepo<String>) {
    assert_that!(repo.insert("Key".into(), &TEST_VALUE)).is_ok();
    assert_that!(repo.get::<_, String>("Key")).is_err_variant(acid_store::Error::Deserialize);
}

#[rstest]
fn list_keys(mut repo: ValueRepo<String>) -> anyhow::Result<()> {
    repo.insert("Key1".into(), &TEST_VALUE)?;
    repo.insert("Key2".into(), &TEST_VALUE)?;
    repo.insert("Key3".into(), &TEST_VALUE)?;

    assert_that!(repo.keys().cloned().collect::<Vec<_>>()).contains_all_of(&[
        &String::from("Key1"),
        &String::from("Key2"),
        &String::from("Key3"),
    ]);

    Ok(())
}

#[rstest]
fn values_removed_on_rollback(mut repo: ValueRepo<String>) -> anyhow::Result<()> {
    repo.insert("test".into(), &TEST_VALUE)?;

    repo.rollback()?;

    assert_that!(repo.contains("test")).is_false();
    assert_that!(repo.get::<_, TestType>("test")).is_err_variant(acid_store::Error::NotFound);

    Ok(())
}

#[rstest]
fn clear_instance_removes_keys(mut repo: ValueRepo<String>) -> anyhow::Result<()> {
    repo.insert("test".into(), &TEST_VALUE)?;

    repo.clear_instance();

    assert_that!(repo.contains("test")).is_false();
    assert_that!(repo.get::<_, TestType>("test")).is_err_variant(acid_store::Error::NotFound);

    Ok(())
}

#[rstest]
fn rollback_after_clear_instance(mut repo: ValueRepo<String>) -> anyhow::Result<()> {
    repo.insert("test".into(), &TEST_VALUE)?;

    repo.commit()?;
    repo.clear_instance();
    repo.rollback()?;

    assert_that!(repo.contains("test")).is_true();
    assert_that!(repo.get::<_, TestType>("test")).is_ok();

    Ok(())
}

#[rstest]
fn verify_valid_repository_is_valid(mut repo: ValueRepo<String>) -> anyhow::Result<()> {
    repo.insert("Test".into(), &TEST_VALUE)?;

    assert_that!(repo.verify()).is_ok_containing(HashSet::new());

    Ok(())
}
