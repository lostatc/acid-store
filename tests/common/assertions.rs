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

use spectral::{AssertionFailure, Spec};
use std::fmt::Debug;

/// An assertion which checks if an `acid_store::Result` has the correct error variant.
pub trait ErrorVariantAssertions {
    fn is_err_variant(&self, expected_value: acid_store::Error);
}

impl<'a, T, E> ErrorVariantAssertions for Spec<'a, Result<T, E>>
where
    T: Debug,
    acid_store::Error: From<E>,
{
    fn is_err_variant(&self, expected_value: acid_store::Error) {
        match self.subject.map_err(acid_store::Error::from) {
            Ok(ref value) => {
                AssertionFailure::from_spec(self)
                    .with_expected(format!("Err({:?})", expected_value))
                    .with_actual(format!("Ok({:?})", value))
                    .fail();
            }

            Err(ref error) => {
                if std::mem::discriminant(error) != std::mem::discriminant(&expected_value) {
                    AssertionFailure::from_spec(self)
                        .with_expected(format!("Err({:?})", &expected_value))
                        .with_actual(format!("Err({:?})", error))
                        .fail();
                }
            }
        }
    }
}
