use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use rstest::*;
use tempfile::{tempdir, TempDir};

/// The minimum size of test data buffers.
const MIN_BUFFER_SIZE: usize = 2048;

/// The maximum size of test data buffers.
const MAX_BUFFER_SIZE: usize = 4096;

/// Return a buffer containing `size` random bytes for testing purposes.
fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    let mut buffer = vec![0u8; size];
    rng.fill_bytes(&mut buffer);
    buffer
}

/// A test fixture which provides a randomly sized buffer of random bytes.
#[fixture]
pub fn buffer() -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    random_bytes(rng.gen_range(MIN_BUFFER_SIZE..MAX_BUFFER_SIZE))
}

/// A test fixture which provides a fixed-size buffer of random bytes.
#[fixture]
pub fn fixed_buffer(#[default(MIN_BUFFER_SIZE)] size: usize) -> Vec<u8> {
    random_bytes(size)
}

/// A test fixture which provides a randomly sized buffer of random bytes.
///
/// The returned buffer is guaranteed to be smaller than the one returned by `buffer`.
#[fixture]
pub fn smaller_buffer() -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    random_bytes(rng.gen_range((MIN_BUFFER_SIZE / 2)..MIN_BUFFER_SIZE))
}

/// A test fixture which provides a randomly sized buffer of random bytes.
///
/// The returned buffer is guaranteed to be larger than the one returned by `buffer`.
#[fixture]
pub fn larger_buffer() -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    random_bytes(rng.gen_range(MAX_BUFFER_SIZE..(MAX_BUFFER_SIZE * 2)))
}

/// A test fixture which provides a temporary directory that is deleted once the test completes.
#[fixture]
pub fn temp_dir() -> TempDir {
    tempdir().unwrap()
}
