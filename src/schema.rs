/*
 * Copyright 2019 Wren Powell
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

table! {
    Blob (id) {
        id -> Integer,
        size -> Integer,
        checksum -> Binary,
        data -> Binary,
    }
}

table! {
    Block (file_id, blob_id, index) {
        file_id -> Integer,
        blob_id -> Integer,
        index -> Integer,
    }
}

table! {
    Directory (file_id) {
        file_id -> Integer,
    }
}

table! {
    ExtendedAttribute (id) {
        id -> Integer,
        file_id -> Integer,
        name -> Text,
        value -> Binary,
    }
}

table! {
    File (id) {
        id -> Integer,
        path -> Text,
        modifiedTime -> Integer,
        permissions -> Nullable<Integer>,
    }
}

table! {
    RegularFile (file_id) {
        file_id -> Integer,
        size -> Integer,
        checksum -> Binary,
    }
}

table! {
    SymbolicLink (file_id) {
        file_id -> Integer,
        target -> Text,
    }
}

joinable!(Block -> Blob (blob_id));
joinable!(Block -> File (file_id));
joinable!(Directory -> File (file_id));
joinable!(ExtendedAttribute -> File (file_id));
joinable!(RegularFile -> File (file_id));
joinable!(SymbolicLink -> File (file_id));

allow_tables_to_appear_in_same_query!(
    Blob,
    Block,
    Directory,
    ExtendedAttribute,
    File,
    RegularFile,
    SymbolicLink,
);
