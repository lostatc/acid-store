use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use hole_punch::{ScanError, SegmentType, SparseFile};

use crate::repo::Object;

/// Copy the contents of the regular file at `path` to the given `object`.
///
/// This attempts to efficiently copies any sparse holes in the file.
///
/// It is assumed that the given `object` will be empty and the seek position will be at the start
/// of the object.
///
/// # Panics
/// - The `object` is not empty.
/// - The seek position of the `object` is not at `0`.
pub fn archive_file(object: &mut Object, path: &Path) -> crate::Result<()> {
    let mut file = File::open(path)?;

    assert!(matches!(object.size(), Ok(0)));
    assert!(matches!(object.seek(SeekFrom::Current(0)), Ok(0)));

    match file.scan_chunks() {
        Ok(segments) => {
            for segment in &segments {
                match segment.segment_type {
                    SegmentType::Hole => {
                        object.commit()?;
                        object.set_len(segment.end)?;
                        object.seek(SeekFrom::End(0))?;
                    }
                    SegmentType::Data => {
                        file.seek(SeekFrom::Start(segment.start))?;
                        let mut file_reader = file.take(segment.end - segment.start);
                        io::copy(&mut file_reader, object)?;
                        file = file_reader.into_inner();
                    }
                }
            }
            object.commit()
        }
        Err(ScanError::UnsupportedFileSystem | ScanError::UnsupportedPlatform) => {
            // Sparse files aren't supported. Perform a naive copy.
            io::copy(&mut file, object)?;
            object.commit()
        }
        Err(ScanError::IO(error)) => Err(crate::Error::Io(error)),
        Err(ScanError::Raw(error)) => Err(crate::Error::Io(io::Error::from_raw_os_error(error))),
    }
}

/// Copy the contents of the given `object` to the regular file at `path`.
///
/// This attempts to efficiently copies any sparse holes in the object.
///
/// It is assumed that the seek position of `object` will be at the start of the object.
///
/// # Panics
/// - The seek position of the `object` is not at `0`.
pub fn extract_file(object: &mut Object, path: &Path) -> crate::Result<()> {
    assert!(matches!(object.seek(SeekFrom::Current(0)), Ok(0)));

    let stats = object.stats()?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;

    for hole in stats.holes() {
        // Copy the bytes before the hole.
        let current_position = object.seek(SeekFrom::Current(0))?;
        let bytes_before_hole = hole.start - current_position;
        let mut object_reader = object.take(bytes_before_hole);
        io::copy(&mut object_reader, &mut file)?;

        // Copy the hole.
        object.set_len(hole.end)?;
        object.seek(SeekFrom::End(0))?;
    }

    // Copy the bytes after the last hole.
    io::copy(object, &mut file)?;

    Ok(())
}
