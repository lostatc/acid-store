/*
 * Copyright 2019-2021 Wren Powell
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

//! A low-level repository type which can be used to implement higher-level repository types
//!
//! This module contains the [`StateRepo`] repository type.
//!
//! The purpose of [`StateRepo`] is not to be used on its own, but to be used to implement new
//! repository types.
//!
//! [`StateRepo`] is like [`KeyRepo`], but differs in two ways:
//!
//! 1. A [`StateRepo`] encapsulates a `State` value which is automatically read from and written to
//! the data store. Unlike data stored in an object, the repository state is stored in memory, and
//! it can be accessed via [`state`] and [`state_mut`]. `State` must implement `Default`, which is
//! the value the repository state will have when the repository is created or when
//! [`clear_instance`] is called.
//!
//! 2. Objects are accessed via [`ObjectKey`] values instead of generic keys. Creating a new object
//! returns an [`ObjectKey`] value which can be used to access the object. These [`ObjectKey`]
//! values are opaque, but they're serializable, meaning that an [`ObjectKey`] can be written to
//! another object or stored in the repository state.
//!
//! [`StateRepo`]: crate::repo::state::StateRepo
//! [`KeyRepo`]: crate::repo::key::KeyRepo
//! [`state`]: crate::repo::state::StateRepo::state
//! [`state_mut`]: crate::repo::state::StateRepo::state_mut
//! [`clear_instance`]: crate::repo::state::StateRepo::clear_instance
//! [`ObjectKey`]: crate::repo::state::ObjectKey

pub use self::info::ObjectKey;
pub use self::repository::StateRepo;

mod info;
mod repository;
