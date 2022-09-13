use spectral::{AssertionFailure, Spec};
use std::fmt::Debug;

/// An assertion which checks if an `acid_store::Result` has the correct error variant.
pub trait ErrorVariantAssertions {
    fn is_err_variant(&self, expected_value: acid_store::Error);
}

impl<'a, T> ErrorVariantAssertions for Spec<'a, acid_store::Result<T>>
where
    T: Debug,
{
    fn is_err_variant(&self, expected_value: acid_store::Error) {
        match &self.subject {
            Ok(value) => {
                AssertionFailure::from_spec(self)
                    .with_expected(format!("Err({:?})", expected_value))
                    .with_actual(format!("Ok({:?})", value))
                    .fail();
            }

            Err(error) => {
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
