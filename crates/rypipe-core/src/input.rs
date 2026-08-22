#![allow(unsafe_code)]

use std::path::Path;

use crate::Result;

/// Owned handle for a memory-mapped file.
#[cfg(feature = "mmap")]
pub struct MmapHandle {
    mmap: memmap2::Mmap,
}

#[cfg(feature = "mmap")]
impl MmapHandle {
    fn new(file: std::fs::File, prefault: bool) -> Result<Self> {
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        #[cfg(unix)]
        {
            if prefault {
                // Pre-fault the entire file into RSS.
                let _ = mmap.advise(memmap2::Advice::WillNeed);
            } else {
                // Let the kernel drop pages behind the sequential reader.
                let _ = mmap.advise(memmap2::Advice::Sequential);
            }
        }
        Ok(MmapHandle { mmap })
    }

    fn as_slice(&self) -> &[u8] {
        &self.mmap[..]
    }
}

/// Input abstraction: either a memory-mapped file or an owned in-memory buffer.
pub enum InputBuffer {
    #[cfg(feature = "mmap")]
    Mmap(MmapHandle),
    Owned(Vec<u8>),
}

impl InputBuffer {
    /// Open a path as an input buffer.
    ///
    /// When `use_mmap` is true and the `"mmap"` feature is enabled, the file
    /// is mapped.  Otherwise it is read into an owned `Vec<u8>`.
    pub fn open(path: &Path, use_mmap: bool, prefault: bool) -> Result<Self> {
        #[cfg(feature = "mmap")]
        {
            if use_mmap {
                let file = std::fs::File::open(path)?;
                return Ok(InputBuffer::Mmap(MmapHandle::new(file, prefault)?));
            }
        }
        let _ = use_mmap;
        let _ = prefault;
        let bytes = std::fs::read(path)?;
        Ok(InputBuffer::Owned(bytes))
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(feature = "mmap")]
            InputBuffer::Mmap(handle) => handle.as_slice(),
            InputBuffer::Owned(bytes) => bytes,
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
