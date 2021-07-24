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

use acid_store::store::DataStore;
use common::*;
use rstest_reuse::{self, *};
use uuid::Uuid;

mod common;

#[apply(data_stores)]
fn read_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id = Uuid::new_v4().into();

    assert_that!(store.read_block(id)).is_ok_containing(None);
    assert_that!(store.write_block(id, &buffer)).is_ok();
    assert_that!(store.read_block(id)).is_ok_containing(Some(buffer));
}

#[apply(data_stores)]
fn overwrite_block(
    #[case] mut store: Box<dyn DataStore>,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) {
    let id = Uuid::new_v4().into();

    assert_that!(store.write_block(id, &first_buffer)).is_ok();
    assert_that!(store.write_block(id, &second_buffer)).is_ok();
    assert_that!(store.read_block(id)).is_ok_containing(Some(second_buffer));
}

#[apply(data_stores)]
fn remove_block(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id = Uuid::new_v4().into();

    assert_that!(store.write_block(id, &buffer)).is_ok();
    assert_that!(store.remove_block(id)).is_ok();
    assert_that!(store.read_block(id)).is_ok_containing(None);
    assert_that!(store.remove_block(Uuid::new_v4().into())).is_ok();
}

#[apply(data_stores)]
fn list_blocks(#[case] mut store: Box<dyn DataStore>, buffer: Vec<u8>) {
    let id1 = Uuid::new_v4().into();
    let id2 = Uuid::new_v4().into();
    let id3 = Uuid::new_v4().into();

    assert_that!(store.list_blocks()).is_ok_containing(Vec::new());

    assert_that!(store.write_block(id1, &buffer)).is_ok();
    assert_that!(store.write_block(id2, &buffer)).is_ok();
    assert_that!(store.write_block(id3, &buffer)).is_ok();

    let list_result = store.list_blocks();
    assert_that!(list_result).is_ok();
    assert_that!(list_result.unwrap()).contains_all_of(&[&id1, &id2, &id3]);
}
