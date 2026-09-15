use std::io::{Read, Seek};
use std::path::Path;

use crate::Result;

/// Maximum decompressed size in bytes (1 GiB). Compressed inputs that would
/// exceed this limit are rejected to prevent decompression-bomb OOM kills.
#[allow(dead_code)] // used only when a decompression feature is enabled
const MAX_DECOMPRESSED_BYTES: u64 = 1 << 30;

/// A `Read` wrapper that aborts once the total bytes read exceeds a limit.
/// Used to cap decompressed output and prevent decompression-bomb OOM kills.
#[allow(dead_code)] // used only when a decompression feature is enabled
struct LimitReader<R> {
    inner: R,
    remaining: u64,
    limit: u64,
}

#[allow(dead_code)] // used only when a decompression feature is enabled
impl<R: Read> LimitReader<R> {
    fn new(inner: R, limit: u64) -> Self {
        Self {
            inner,
            remaining: limit,
            limit,
        }
    }
}

impl<R: Read> Read for LimitReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let allowed = usize::try_from(self.remaining)
            .unwrap_or(usize::MAX)
            .saturating_add(1)
            .min(buf.len());
        let n = self.inner.read(&mut buf[..allowed])?;
        if n as u64 > self.remaining {
            self.remaining = 0;
            return Err(std::io::Error::other(format!(
                "decompressed output exceeds {} byte limit (possible decompression bomb)",
                self.limit
            )));
        }
        self.remaining -= n as u64;
        Ok(n)
    }
}

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
    pub(crate) mmap: memmap2::Mmap,
    pub(crate) file: std::fs::File,
}

#[cfg(feature = "mmap")]
impl MmapHandle {
    fn new(file: std::fs::File, prefault: bool) -> Result<Self> {
        // The input must not be modified or truncated while mapped.
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
        #[cfg(not(unix))]
        let _ = prefault;
        Ok(MmapHandle { mmap, file })
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

fn detect_compression(file: &mut std::fs::File) -> Result<Option<Compression>> {
    let mut magic = [0u8; 4];
    let n = file.read(&mut magic)?;
    file.rewind()?;
    #[cfg(feature = "gzip")]
    if n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
        return Ok(Some(Compression::Gzip));
    }
    #[cfg(feature = "zstd")]
    if n >= 4 && magic == [0x28, 0xb5, 0x2f, 0xfd] {
        return Ok(Some(Compression::Zstd));
    }
    #[cfg(feature = "lz4")]
    if n >= 4 && magic == [0x04, 0x22, 0x4d, 0x18] {
        return Ok(Some(Compression::Lz4));
    }
    let _ = n;
    Ok(None)
}

/// Decompress `file` with `codec` into an owned buffer.
/// Aborts early if decompressed output exceeds [`MAX_DECOMPRESSED_BYTES`].
#[cfg_attr(
    not(any(feature = "gzip", feature = "zstd", feature = "lz4")),
    allow(unused_variables, unused_mut)
)]
fn decompress(mut file: std::fs::File, codec: Compression) -> Result<Vec<u8>> {
    match codec {
        #[cfg(feature = "gzip")]
        Compression::Gzip => {
            let mut out = Vec::new();
            let decoder = flate2::read::GzDecoder::new(&mut file);
            LimitReader::new(decoder, MAX_DECOMPRESSED_BYTES).read_to_end(&mut out)?;
            Ok(out)
        }
        #[cfg(feature = "zstd")]
        Compression::Zstd => {
            let mut out = Vec::new();
            let decoder = zstd::stream::read::Decoder::new(&mut file)?;
            LimitReader::new(decoder, MAX_DECOMPRESSED_BYTES).read_to_end(&mut out)?;
            Ok(out)
        }
        #[cfg(feature = "lz4")]
        Compression::Lz4 => {
            let mut out = Vec::new();
            let decoder = lz4_flex::frame::FrameDecoder::new(&mut file);
            LimitReader::new(decoder, MAX_DECOMPRESSED_BYTES).read_to_end(&mut out)?;
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
        let mut file = std::fs::File::open(path)?;
        if let Some(codec) = detect_compression(&mut file)? {
            return Ok(InputBuffer::Owned(decompress(file, codec)?));
        }

        #[cfg(feature = "mmap")]
        {
            if use_mmap {
                return Ok(InputBuffer::Mmap(MmapHandle::new(file, prefault)?));
            }
        }
        let _ = use_mmap;
        let _ = prefault;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};

    #[test]
    fn decompressed_limit_accepts_exactly_one_gib_without_retaining_output() {
        let mut reader = LimitReader::new(
            io::repeat(0).take(MAX_DECOMPRESSED_BYTES),
            MAX_DECOMPRESSED_BYTES,
        );
        assert_eq!(
            io::copy(&mut reader, &mut io::sink()).unwrap(),
            MAX_DECOMPRESSED_BYTES
        );
    }

    #[test]
    fn decompressed_limit_rejects_one_extra_byte() {
        let mut reader = LimitReader::new(
            io::repeat(0).take(MAX_DECOMPRESSED_BYTES + 1),
            MAX_DECOMPRESSED_BYTES,
        );
        let error = io::copy(&mut reader, &mut io::sink()).unwrap_err();
        assert!(error
            .to_string()
            .contains(&MAX_DECOMPRESSED_BYTES.to_string()));
        assert_eq!(reader.inner.limit(), 0);
    }

    #[test]
    fn limit_reader_does_not_overread_the_limit_or_touch_empty_buffers() {
        let mut reader = LimitReader::new(&b"123456789"[..], 3);
        assert_eq!(reader.read(&mut []).unwrap(), 0);
        let error = reader.read(&mut [0; 1024]).unwrap_err();
        assert!(error.to_string().contains("3 byte limit"));
        assert_eq!(reader.inner, b"56789");
        let mut empty = LimitReader::new(&b""[..], 0);
        assert_eq!(empty.read(&mut [0; 1]).unwrap(), 0);
    }

    #[test]
    fn compressed_expansion_hits_the_same_reader_limit() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        encoder.write_all(&[0; 128 * 1024]).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() < 1024);
        let decoder = flate2::read::GzDecoder::new(compressed.as_slice());
        let mut reader = LimitReader::new(decoder, 4096);
        let error = io::copy(&mut reader, &mut io::sink()).unwrap_err();
        assert!(error.to_string().contains("4096 byte limit"));
    }
}
