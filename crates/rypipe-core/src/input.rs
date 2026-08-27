#![allow(unsafe_code)]

use std::io::Read;
use std::path::Path;

use crate::Result;

/// Compression codecs recognized by leading magic bytes. Each codec is
/// compiled in only when its Cargo feature (`gzip`, `zstd`, `lz4`) is
/// enabled; inputs are detected independently of file extension.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // variants exist only under their cargo features
enum Compression {
    Gzip,
    Zstd,
    Lz4,
}

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

/// Read the first four bytes of `path` and match them against the known
/// codec magics (gzip `1f 8b`, zstd `28 b5 2f fd`, lz4 frame `04 22 4d 18`).
/// Unreadable files yield `None` and surface their real error later via the
/// normal open path.
fn detect_compression(path: &Path) -> Option<Compression> {
    let mut magic = [0u8; 4];
    let n = std::fs::File::open(path).ok()?.read(&mut magic).ok()?;
    #[cfg(feature = "gzip")]
    if n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
        return Some(Compression::Gzip);
    }
    #[cfg(feature = "zstd")]
    if n >= 4 && magic == [0x28, 0xb5, 0x2f, 0xfd] {
        return Some(Compression::Zstd);
    }
    #[cfg(feature = "lz4")]
    if n >= 4 && magic == [0x04, 0x22, 0x4d, 0x18] {
        return Some(Compression::Lz4);
    }
    let _ = n;
    None
}

/// Decompress `path` with `codec` into an owned buffer.
#[cfg_attr(
    not(any(feature = "gzip", feature = "zstd", feature = "lz4")),
    allow(unused_variables)
)]
fn decompress(path: &Path, codec: Compression) -> Result<Vec<u8>> {
    match codec {
        #[cfg(feature = "gzip")]
        Compression::Gzip => {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(std::fs::File::open(path)?).read_to_end(&mut out)?;
            Ok(out)
        }
        #[cfg(feature = "zstd")]
        Compression::Zstd => {
            let mut out = Vec::new();
            zstd::stream::read::Decoder::new(std::fs::File::open(path)?)?
                .read_to_end(&mut out)?;
            Ok(out)
        }
        #[cfg(feature = "lz4")]
        Compression::Lz4 => {
            let mut out = Vec::new();
            lz4_flex::frame::FrameDecoder::new(std::fs::File::open(path)?)
                .read_to_end(&mut out)?;
            Ok(out)
        }
        // Only reachable when a codec's cargo feature is disabled; detection
        // never selects disabled codecs, so this is defensive.
        #[allow(unreachable_patterns)]
        _ => Err(crate::Error::Io(std::io::Error::other(format!(
            "{codec:?} input detected but its cargo feature is not enabled"
        )))),
    }
}

impl InputBuffer {
    /// Open a path as an input buffer.
    ///
    /// When the leading magic bytes identify a compression codec whose Cargo
    /// feature is enabled, the file is transparently decompressed into an
    /// owned buffer; all execution modes then operate on the decompressed
    /// bytes. Otherwise, when `use_mmap` is true and the `"mmap"` feature is
    /// enabled, the file is mapped; otherwise it is read into memory.
    pub fn open(path: &Path, use_mmap: bool, prefault: bool) -> Result<Self> {
        if let Some(codec) = detect_compression(path) {
            return Ok(InputBuffer::Owned(decompress(path, codec)?));
        }

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
