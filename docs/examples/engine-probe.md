# Engine probe

`engine_probe.rs` is a small protocol for comparing input paths and record
boundary handling without mixing several experiments in one timing.

## Why this shape

The program accepts `boundary`, `bytes`, `parallel`, `mmap`, `stream`,
`stream-bytes`, `iterator`, or `stream-parallel`, plus a file. Optional
arguments select record shape: `plain`, `continued`, `comments`, or `blank`,
then payload width, memory budget in bytes, and column count. It prints
`ready` and waits for one stdin line before measuring. A harness can launch the
process, synchronize startup, then begin its clock outside engine
initialization.

`boundary` compares the declarative `find_next_record_boundary` helper with a
manual newline scan. The other modes run the same `Records` splitter/parser
through `Pipeline`: in-memory bytes, four-chunk parallel bytes, mmap-planned
path input, or a 10 MiB memory-budget stream. `continued` makes each logical
record span two physical lines joined by a backslash rule. `comments` skips
`#` lines. `blank` uses blank-line records. Width and column count drive the
generated contract checked by the consumer.

The `Check` consumer validates every output string has 128 `x` bytes, counts
rows and payload bytes, and records the largest Arrow batch. The final row
assertion checks the expected 129-byte or 131-byte record width. JSON output
is easy for a benchmark harness to collect.

## Run it

```console
cargo run --release -p rypipe-core --example engine_probe -- boundary path/to/file plain 128 10485760 1
cargo run --release -p rypipe-core --example engine_probe -- bytes path/to/file continued 256 10485760 2
cargo run --release -p rypipe-core --example engine_probe -- parallel path/to/file comments 128 10485760 1
cargo run --release -p rypipe-core --example engine_probe -- mmap path/to/file blank 128 10485760 1
cargo run --release -p rypipe-core --example engine_probe -- stream path/to/file continued 128 10485760 1
cargo run --release -p rypipe-core --example engine_probe -- stream-bytes path/to/file plain 128 10485760 1
cargo run --release -p rypipe-core --example engine_probe -- iterator path/to/file plain 128 10485760 1
cargo run --release -p rypipe-core --example engine_probe -- stream-parallel path/to/file plain 128 10485760 1
```

After `ready`, send one newline to release the measurement. The probe checks
its synthetic record contract, not arbitrary parser semantics. Boundary timing
compares equivalent scans but does not measure full pipeline cost. Mmap and
stream results depend on OS cache state and available memory.

Source: [`engine_probe.rs`](../../crates/rypipe-core/examples/engine_probe.rs).
