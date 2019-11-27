/*
 * Copyright 2019 Garrett Powell
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

use std::io::Write;
use std::path::PathBuf;

use chrono::NaiveDateTime;
use diesel::backend::Backend;
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types::Text;

#[derive(Identifiable, Queryable, PartialEq, Debug)]
pub struct File {
    pub id: i32,
    pub path: String,
    pub modified_time: NaiveDateTime,
    pub permissions: Option<i32>,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[primary_key(file_id)]
#[belongs_to(File)]
pub struct RegularFile {
    pub file_id: i32,
    pub size: u64,
    pub checksum: Vec<u8>,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[primary_key(file_id)]
#[belongs_to(File)]
pub struct SymbolicLink {
    pub file_id: i32,
    pub target: String,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[primary_key(file_id)]
#[belongs_to(File)]
pub struct Directory {
    pub file_id: i32
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[belongs_to(File)]
pub struct ExtendedAttribute {
    pub id: i32,
    pub file_id: i32,
    pub name: String,
    pub value: Vec<u8>,
}

#[derive(Identifiable, Queryable, PartialEq, Debug)]
pub struct Blob {
    pub id: i32,
    pub size: u64,
    pub checksum: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[primary_key(file_id, blob_id, index)]
#[belongs_to(Blob)]
#[belongs_to(File)]
pub struct Block {
    pub file_id: i32,
    pub blob_id: i32,
    pub index: i32,
}
