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

use std::io::{self, Write};
use std::mem::replace;

use cdchunking::{ChunkerImpl, ZPAQ};
use serde::{Deserialize, Serialize};

/// A method for chunking data in a repository.
///
/// Data is deduplicated, read into memory, and written to the data store in chunks. This value
/// determines how data is split into chunks and how large those chunks are.
///
/// The chunk size affects deduplication ratios, memory usage, and I/O performance. Some
/// experimentation may be required to determine the optimal chunk size for a given workload.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Chunking {
    /// Split data into fixed-size chunks.
    ///
    /// This chunking method may provide better performance than `Zpaq`, but does not provide
    /// content-defined deduplication and will probably result in worse deduplication ratios.
    Fixed {
        /// The size of each chunk in bytes.
        size: usize,
    },

    /// Split data using the ZPAQ content-defined chunking algorithm.
    ///
    /// This chunking method provides content-defined deduplication, which allows for better
    /// deduplication ratios than `Fixed`. However, performance may be worse.
    Zpaq {
        /// The average chunk size, which is 2^`bits` bytes.
        ///
        /// For example, a value of `20` will result in an average chunk size of 1MiB
        /// (2^20 = 1048576).
        bits: usize,
    },
}

impl Chunking {
    /// Return a chunker for this chunking method.
    pub(super) fn to_chunker(&self) -> Box<dyn ChunkerImpl> {
        match self {
            Chunking::Fixed { size } => Box::new(FixedChunker::new(*size)),
            Chunking::Zpaq { bits } => Box::new(ZPAQ::new(*bits)),
        }
    }
}

/// A `ChunkerImpl` which chunks data into fixed-size chunks.
pub struct FixedChunker {
    chunk_size: usize,
    bytes_read: usize,
}

impl FixedChunker {
    /// Return a new instance which chunks data using the given `chunk_size`.
    pub fn new(chunk_size: usize) -> Self {
        FixedChunker {
            chunk_size,
            bytes_read: 0,
        }
    }
}

impl ChunkerImpl for FixedChunker {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        if self.bytes_read + data.len() < self.chunk_size {
            None
        } else {
            Some((self.bytes_read + data.len()) - self.chunk_size)
        }
    }

    fn reset(&mut self) {
        self.bytes_read = 0;
    }
}

/// A chunker which partitions data written to it into chunks.
pub struct IncrementalChunker {
    chunker: Box<dyn ChunkerImpl>,
    buffer: Vec<u8>,
    chunks: Vec<Vec<u8>>,
}

impl IncrementalChunker {
    /// Return a new instance which uses the given `chunker` to determine chunk boundaries.
    pub fn new(chunker: Box<dyn ChunkerImpl>) -> Self {
        Self {
            chunker,
            buffer: Vec::new(),
            chunks: Vec::new(),
        }
    }

    /// Return the data which has been written to this chunker separated into chunks.
    ///
    /// This may not return all the bytes which have been written to this chunker; some data may
    /// still be buffered internally. Calling `flush` will make this method return the remaining
    /// buffered data as a new chunk.
    pub fn chunks(&mut self) -> Vec<Vec<u8>> {
        replace(&mut self.chunks, Vec::new())
    }

    /// Clear all the data in the chunker.
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.chunks.clear();
        self.chunker.reset();
    }

    /// Return whether this chunker contains no data.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty() && self.chunks.is_empty()
    }
}

impl Write for IncrementalChunker {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut unchunked_data = buf;

        loop {
            match self.chunker.find_boundary(unchunked_data) {
                None => {
                    self.buffer.extend_from_slice(unchunked_data);
                    return Ok(buf.len());
                }
                Some(index) => {
                    self.buffer.extend_from_slice(&unchunked_data[..index]);
                    let new_chunk = replace(&mut self.buffer, Vec::new());
                    self.chunks.push(new_chunk);
                    unchunked_data = &unchunked_data[index..];
                    self.chunker.reset();
                }
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            let new_chunk = replace(&mut self.buffer, Vec::new());
            self.chunks.push(new_chunk);
        }
        self.chunker.reset();
        Ok(())
    }
}
