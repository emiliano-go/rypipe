# I/O tuning

Parsing cannot outrun the I/O subsystem. `rypipe` supports memory-mapped and buffered input, plus prefault options. Choosing the right combination depends on file size, RAM, and storage class.

## `mmap` vs buffered reads

`use_mmap=True` (the default) maps the file into virtual memory. The kernel loads pages on demand and caches them in the OS page cache.

| Mode | Best for | Behavior |
|------|----------|----------|
| `use_mmap=True` | Files that fit in RAM or are expected to be cached | Kernel manages paging; parser sees a byte slice. |
| `use_mmap=False` | Large cold files or portability constraints | Reads the entire file into a `Vec<u8>`. |

For files that fit in RAM, mmap is usually fastest because it avoids an explicit copy into user space. For cold files larger than RAM, buffered reads may give smoother throughput because the parser avoids page-fault stalls.

## Prefault

`prefault=True` uses `MADV_WILLNEED` to fault the whole file up front. This is fastest when the file fits in RAM and you want to hide latency behind sequential reads. It can be harmful for files larger than RAM because it forces the kernel to read pages that may be evicted before use.

`prefault=False` uses `MADV_SEQUENTIAL` so the kernel can drop pages behind the reader. This is better for RSS-sensitive workloads and for streaming large files.

| Combination | Best for |
|-------------|----------|
| `use_mmap=True, prefault=True` | Speed when the file fits in RAM. |
| `use_mmap=True, prefault=False` | Large files where RSS matters. |
| `use_mmap=False` | Portability; reads into a `Vec<u8>`. |

## OS page cache

The page cache is the biggest factor for repeated reads. If a file has been read recently, it is probably in cache, and mmap or buffered reads will be fast regardless of the underlying storage.

For one-off reads of large files, storage bandwidth is the limit. A modern NVMe SSD can sustain 3-7 GB/s sequential reads; a SATA SSD is closer to 500 MB/s; network storage varies widely.

## SSD vs NVMe vs network storage

| Storage | Typical sequential read | Implications |
|---------|------------------------|--------------|
| NVMe SSD | 3-7 GB/s | Parser can be the bottleneck; parallel mode helps. |
| SATA SSD | 400-600 MB/s | May be I/O-bound for simple formats; still fast enough for most XML/JSON. |
| Network (NFS/S3) | 50-500 MB/s | Latency and throughput vary; streaming may be safer than mmap. |
| Cold object storage | <100 MB/s | Consider downloading first or using buffered reads. |

On network storage, mmap can trigger many small page faults over a high-latency link. Buffered reads with a large readahead are usually smoother.

## Drop before parse

In bounded stream mode, the input buffer is dropped before the parse phase begins. This releases mapped pages before downstream work starts. Combined with `prefault=False`, this keeps peak memory close to the parse budget even when the file is much larger than RAM.

## Summary

- Use `mmap` + `prefault=True` for cached or RAM-resident files.
- Use `mmap` + `prefault=False` for large streaming files.
- Use buffered reads for network or portable deployments.
- Match the parser throughput to storage bandwidth; do not over-parallelize an I/O-bound workload.
