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

use std::io::{self, Write};
use std::mem::replace;

use cdchunking::ChunkerImpl;

use super::object::ChunkHash;

/// A value which writes chunks of data identified by a checksum.
pub trait ChunkWriter {
    /// Write the given `data` as a new chunk and returns its checksum.
    ///
    /// If a chunk with the given `data` already exists, its checksum may be returned without
    /// writing any new data.
    fn write_chunk(&mut self, data: &[u8]) -> io::Result<ChunkHash>;
}

/// A value which reads chunks of data identified by a checksum.
pub trait ChunkReader {
    /// Return the bytes of the chunk with the given checksum or `None` if there is none.
    fn read_chunk(&self, checksum: &ChunkHash) -> io::Result<Vec<u8>>;
}

/// A chunker which partitions data written to it into chunks.
pub struct IncrementalChunker<T: ChunkerImpl> {
    chunker: T,
    buffer: Vec<u8>,
    chunks: Vec<Vec<u8>>,
}

impl<T: ChunkerImpl> IncrementalChunker<T> {
    /// Return a new instance with uses the given `chunker` to determine chunk boundaries.
    pub fn new(chunker: T) -> Self {
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
}

impl<T: ChunkerImpl> Write for IncrementalChunker<T> {
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
        let new_chunk = replace(&mut self.buffer, Vec::new());
        self.chunks.push(new_chunk);
        self.chunker.reset();
        Ok(())
    }
}
