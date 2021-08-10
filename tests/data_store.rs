/*
 * Copyright 2019-2020 Wren Powell
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#![cfg(all(feature = "encryption", feature = "compression"))]

use acid_store::store::{BlockKey, BlockType, DataStore};
use rstest_reuse::{self, *};
use serial_test::serial;
use uuid::Uuid;

use common::*;

mod common;

#[apply(data_stores)]
#[serial(data_store)]
fn read_data_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id = Uuid::new_v4().into();

    assert_that!(store.read_block(BlockKey::Data(id))).is_ok_containing(None);
    assert_that!(store.write_block(BlockKey::Data(id), &buffer)).is_ok();
    assert_that!(store.read_block(BlockKey::Data(id))).is_ok_containing(Some(buffer));
}

#[apply(data_stores)]
#[serial(data_store)]
fn read_lock_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id = Uuid::new_v4().into();

    assert_that!(store.read_block(BlockKey::Lock(id))).is_ok_containing(None);
    assert_that!(store.write_block(BlockKey::Lock(id), &buffer)).is_ok();
    assert_that!(store.read_block(BlockKey::Lock(id))).is_ok_containing(Some(buffer));
}

#[apply(data_stores)]
#[serial(data_store)]
fn read_header_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id = Uuid::new_v4().into();

    assert_that!(store.read_block(BlockKey::Header(id))).is_ok_containing(None);
    assert_that!(store.write_block(BlockKey::Header(id), &buffer)).is_ok();
    assert_that!(store.read_block(BlockKey::Header(id))).is_ok_containing(Some(buffer));
}

#[apply(data_stores)]
#[serial(data_store)]
fn read_super_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    assert_that!(store.read_block(BlockKey::Super)).is_ok_containing(None);
    assert_that!(store.write_block(BlockKey::Super, &buffer)).is_ok();
    assert_that!(store.read_block(BlockKey::Super)).is_ok_containing(Some(buffer));
}

#[apply(data_stores)]
#[serial(data_store)]
fn read_version_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    assert_that!(store.read_block(BlockKey::Version)).is_ok_containing(None);
    assert_that!(store.write_block(BlockKey::Version, &buffer)).is_ok();
    assert_that!(store.read_block(BlockKey::Version)).is_ok_containing(Some(buffer));
}

#[apply(data_stores)]
#[serial(data_store)]
fn overwrite_data_block(
    #[case] mut store: Box<dyn DataStore>,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) {
    let id = Uuid::new_v4().into();

    assert_that!(store.write_block(BlockKey::Data(id), &first_buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Data(id), &second_buffer)).is_ok();
    assert_that!(store.read_block(BlockKey::Data(id))).is_ok_containing(Some(second_buffer));
}

#[apply(data_stores)]
#[serial(data_store)]
fn overwrite_lock_block(
    #[case] mut store: Box<dyn DataStore>,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) {
    let id = Uuid::new_v4().into();

    assert_that!(store.write_block(BlockKey::Lock(id), &first_buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Lock(id), &second_buffer)).is_ok();
    assert_that!(store.read_block(BlockKey::Lock(id))).is_ok_containing(Some(second_buffer));
}

#[apply(data_stores)]
#[serial(data_store)]
fn overwrite_header_block(
    #[case] mut store: Box<dyn DataStore>,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) {
    let id = Uuid::new_v4().into();

    assert_that!(store.write_block(BlockKey::Header(id), &first_buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Header(id), &second_buffer)).is_ok();
    assert_that!(store.read_block(BlockKey::Header(id))).is_ok_containing(Some(second_buffer));
}

#[apply(data_stores)]
#[serial(data_store)]
fn overwrite_super_block(
    #[case] mut store: Box<dyn DataStore>,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) {
    assert_that!(store.write_block(BlockKey::Super, &first_buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Super, &second_buffer)).is_ok();
    assert_that!(store.read_block(BlockKey::Super)).is_ok_containing(Some(second_buffer));
}

#[apply(data_stores)]
#[serial(data_store)]
fn overwrite_version_block(
    #[case] mut store: Box<dyn DataStore>,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) {
    assert_that!(store.write_block(BlockKey::Version, &first_buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Version, &second_buffer)).is_ok();
    assert_that!(store.read_block(BlockKey::Version)).is_ok_containing(Some(second_buffer));
}

#[apply(data_stores)]
#[serial(data_store)]
fn remove_data_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id = Uuid::new_v4().into();

    assert_that!(store.write_block(BlockKey::Data(id), &buffer)).is_ok();
    assert_that!(store.remove_block(BlockKey::Data(id))).is_ok();
    assert_that!(store.read_block(BlockKey::Data(id))).is_ok_containing(None);
    assert_that!(store.remove_block(BlockKey::Data(Uuid::new_v4().into()))).is_ok();
}

#[apply(data_stores)]
#[serial(data_store)]
fn remove_lock_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id = Uuid::new_v4().into();

    assert_that!(store.write_block(BlockKey::Lock(id), &buffer)).is_ok();
    assert_that!(store.remove_block(BlockKey::Lock(id))).is_ok();
    assert_that!(store.read_block(BlockKey::Lock(id))).is_ok_containing(None);
    assert_that!(store.remove_block(BlockKey::Lock(Uuid::new_v4().into()))).is_ok();
}

#[apply(data_stores)]
#[serial(data_store)]
fn remove_header_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id = Uuid::new_v4().into();

    assert_that!(store.write_block(BlockKey::Header(id), &buffer)).is_ok();
    assert_that!(store.remove_block(BlockKey::Header(id))).is_ok();
    assert_that!(store.read_block(BlockKey::Header(id))).is_ok_containing(None);
    assert_that!(store.remove_block(BlockKey::Header(Uuid::new_v4().into()))).is_ok();
}

#[apply(data_stores)]
#[serial(data_store)]
fn remove_super_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    assert_that!(store.write_block(BlockKey::Super, &buffer)).is_ok();
    assert_that!(store.remove_block(BlockKey::Super)).is_ok();
    assert_that!(store.read_block(BlockKey::Super)).is_ok_containing(None);
}

#[apply(data_stores)]
#[serial(data_store)]
fn remove_version_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    assert_that!(store.write_block(BlockKey::Version, &buffer)).is_ok();
    assert_that!(store.remove_block(BlockKey::Version)).is_ok();
    assert_that!(store.read_block(BlockKey::Version)).is_ok_containing(None);
}

#[apply(data_stores)]
#[serial(data_store)]
fn list_data_blocks(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id1 = Uuid::new_v4().into();
    let id2 = Uuid::new_v4().into();
    let id3 = Uuid::new_v4().into();

    assert_that!(store.list_blocks(BlockType::Data)).is_ok_containing(Vec::new());

    assert_that!(store.write_block(BlockKey::Data(id1), &buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Data(id2), &buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Data(id3), &buffer)).is_ok();

    assert_that!(store.write_block(BlockKey::Lock(Uuid::new_v4().into()), &buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Header(Uuid::new_v4().into()), &buffer)).is_ok();

    let list_result = store.list_blocks(BlockType::Data);
    assert_that!(list_result).is_ok();
    assert_that!(list_result.unwrap()).contains_all_of(&[&id1, &id2, &id3]);
}

#[apply(data_stores)]
#[serial(data_store)]
fn list_lock_blocks(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id1 = Uuid::new_v4().into();
    let id2 = Uuid::new_v4().into();
    let id3 = Uuid::new_v4().into();

    assert_that!(store.list_blocks(BlockType::Lock)).is_ok_containing(Vec::new());

    assert_that!(store.write_block(BlockKey::Lock(id1), &buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Lock(id2), &buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Lock(id3), &buffer)).is_ok();

    assert_that!(store.write_block(BlockKey::Data(Uuid::new_v4().into()), &buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Header(Uuid::new_v4().into()), &buffer)).is_ok();

    let list_result = store.list_blocks(BlockType::Lock);
    assert_that!(list_result).is_ok();
    assert_that!(list_result.unwrap()).contains_all_of(&[&id1, &id2, &id3]);
}

#[apply(data_stores)]
#[serial(data_store)]
fn list_header_blocks(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id1 = Uuid::new_v4().into();
    let id2 = Uuid::new_v4().into();
    let id3 = Uuid::new_v4().into();

    assert_that!(store.list_blocks(BlockType::Header)).is_ok_containing(Vec::new());

    assert_that!(store.write_block(BlockKey::Header(id1), &buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Header(id2), &buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Header(id3), &buffer)).is_ok();

    assert_that!(store.write_block(BlockKey::Data(Uuid::new_v4().into()), &buffer)).is_ok();
    assert_that!(store.write_block(BlockKey::Lock(Uuid::new_v4().into()), &buffer)).is_ok();

    let list_result = store.list_blocks(BlockType::Header);
    assert_that!(list_result).is_ok();
    assert_that!(list_result.unwrap()).contains_all_of(&[&id1, &id2, &id3]);
}
