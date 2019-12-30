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

use std::sync::Once;

use sodiumoxide::init as sodiumoxide_init;

// A synchronization primitive for global initialization.
static INIT: Once = Once::new();

/// Initialize the environment for this crate.
///
/// This function should be called before any other in this crate. This function can be called more
/// than once.
pub fn init() {
    INIT.call_once(|| {
        sodiumoxide_init().expect("Failed to initialize environment.");
    });
}
