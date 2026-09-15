# Engine fuzz targets

Install Rust nightly and `cargo-fuzz`, then run from the repository root:

```sh
cargo +nightly fuzz run fuzz_boundary -- -max_total_time=60
cargo +nightly fuzz run fuzz_splitter -- -max_total_time=60
cargo +nightly fuzz run fuzz_parser -- -max_total_time=60
cargo +nightly fuzz run fuzz_validate -- -max_total_time=60
```

On Windows, use an x64 Visual Studio developer shell with AddressSanitizer
installed. The harness compiles the adapters' actual Rust source files as
modules, with their Python bridges disabled. This avoids cargo-fuzz's
`/include:main` flag being applied to the adapters' Python shared libraries.

Targets check declarative boundary bounds, real-adapter split invariants,
parser-to-Arrow row consistency, and UTF-8 validation against the standard
library. Failures are saved under `fuzz/artifacts`; minimize them with
`cargo +nightly fuzz tmin TARGET ARTIFACT`, then add a regression test.

CI runs 30-second smoke campaigns on every change. Weekly scheduled and
manual campaigns run each target for 10 minutes, restore
`fuzz/corpus/<target>` from the previous run, and upload `fuzz/artifacts` even
when a target fails. Keep minimized failures as regression tests; uploaded
artifacts are retained by CI for triage.

These targets exercise Rust parsers directly. Python integration is covered
by `tests/adapters` and `crates/rypipe-python/tests`.
