# XML report fixture

[`report.xml`](report.xml) is the small XML fixture used by documentation
examples. It gives snippets a concrete `row_tag="Row"` input without making
the docs depend on a downloaded file.

The fixture is sample data, not an executable example and not a benchmark
corpus. Keep its fields aligned with snippets that read `id`, `name`,
`amount`, or `status`. Larger performance claims belong to the Rust benchmark
pages, where the generator and measurement rules are documented.
