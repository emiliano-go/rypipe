use super::*;

#[cfg(feature = "mmap")]
pub(super) fn discover_schema_for_file<P: RecordParser>(
    input: &crate::input::MmapHandle,
    splitter: &dyn Splitter,
    parser: &P,
    plan: &ExecutionPlan,
    budget: MemoryBudget,
) -> Result<FrozenSchema> {
    let bytes = &input.mmap[..];
    let opts = DiscoveryOpts::default();
    use std::hash::{Hash, Hasher};
    let mut file = input.file.try_clone()?;
    let mut buffer = Vec::new();
    let allowance = budget.share(4);
    let mut hasher = rustc_hash::FxHasher::default();
    for range in crate::schema::signature_chunks(bytes.len(), &opts) {
        allowance.check(range.len())?;
        buffer.resize(range.len(), 0);
        allowance.check(buffer.capacity())?;
        file.seek(SeekFrom::Start(range.start as u64))?;
        file.read_exact(&mut buffer)?;
        buffer.hash(&mut hasher);
    }
    let signature = (bytes.len() as u64, hasher.finish());
    let cached = crate::schema::SCHEMA_CACHE
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&signature)
        .cloned();
    if let Some(order) = cached {
        crate::schema::SCHEMA_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
        return Ok(FrozenSchema::from_discovered(&order, plan));
    }
    crate::schema::SCHEMA_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    let start = std::time::Instant::now();
    let chunk_size = (budget.bytes() / 8).max(1);
    let count = bytes
        .len()
        .div_ceil(chunk_size)
        .clamp(1, crate::MAX_SPLIT_CHUNKS);
    let points = splitter.find_split_points(bytes, count);
    let ranges = crate::decoder::split_points_to_ranges(&points, bytes.len());
    let planning_bytes = points
        .capacity()
        .saturating_mul(std::mem::size_of::<usize>())
        .saturating_add(ranges.capacity() * std::mem::size_of::<std::ops::Range<usize>>());
    allowance.check(planning_bytes)?;
    let windows = crate::schema::dynamic_window_count(bytes.len() as u64);
    let window_bytes = crate::schema::dynamic_window_size(bytes.len() as u64);
    let mut sink = DiscoverySink::new();
    for range in ranges {
        if windows > 0
            && !(0..windows).any(|i| {
                let offset = bytes.len() / windows * i;
                range.start < offset.saturating_add(window_bytes) && range.end > offset
            })
            && range.end <= bytes.len().saturating_sub(window_bytes)
        {
            continue;
        }
        allowance.check(range.len().saturating_add(planning_bytes))?;
        buffer.resize(range.len(), 0);
        file.seek(SeekFrom::Start(range.start as u64))?;
        file.read_exact(&mut buffer)?;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_chunk_generic(&buffer, &mut sink)
        }))
        .map_err(|_| crate::Error::Parser("parser panicked during schema discovery".into()))?;
        let order = sink.order.borrow();
        let seen = sink.seen.borrow();
        let storage = order.capacity() * std::mem::size_of::<String>()
            + order.iter().map(String::capacity).sum::<usize>()
            + seen.capacity() * (std::mem::size_of::<Box<str>>() + 1)
            + seen.iter().map(|name| name.len()).sum::<usize>();
        allowance.check(
            storage
                .saturating_add(buffer.capacity())
                .saturating_add(planning_bytes),
        )?;
    }
    let order = sink.into_order();
    let schema = FrozenSchema::from_discovered(&order, plan);
    if !order.is_empty() {
        crate::schema::insert_schema_cache(signature, Arc::new(order));
    }
    DISCOVERY_NS.store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    Ok(schema)
}

static DISCOVERY_NS: AtomicU64 = AtomicU64::new(0);

/// Return the elapsed time (in nanoseconds) spent in schema discovery.
pub fn discovery_profile() -> u64 {
    DISCOVERY_NS.load(Ordering::Relaxed)
}
/// Reset the discovery timer to zero.
pub fn reset_discovery_profile() {
    DISCOVERY_NS.store(0, Ordering::Relaxed);
}

/// Options for parallel streaming.
pub struct ParallelStreamOpts {
    /// Number of worker threads.
    pub threads: usize,
    /// Whether to preserve row order (default: true).
    pub ordered: bool,
    /// Reorder memory limit in multiples of the parsing budget (0 = threads).
    /// Ordered reads return an error if this limit is exceeded.
    /// Workers may run at most this many chunks ahead of the next output chunk.
    pub max_reorder: usize,
    /// Explicit schema.  If `Some`, no discovery pass is needed.
    /// Workers pre-size all columns from construction.
    pub schema: Option<FrozenSchema>,
}

impl Default for ParallelStreamOpts {
    fn default() -> Self {
        Self {
            threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            ordered: true,
            max_reorder: 0, // 0 = use threads value
            schema: None,
        }
    }
}

/// Discovery sink that collects raw field names in encounter order.
/// `needs_value=false` keeps the scanner in locate-only mode (no value
/// extraction), but we still need `resolve()` calls, so `needs_resolve`
/// stays true. Capture happens in `resolve()` via interior mutability.
struct DiscoverySink {
    seen: std::cell::RefCell<rustc_hash::FxHashSet<Box<str>>>,
    order: std::cell::RefCell<Vec<String>>,
}

impl DiscoverySink {
    fn new() -> Self {
        Self {
            seen: std::cell::RefCell::new(rustc_hash::FxHashSet::default()),
            order: std::cell::RefCell::new(Vec::new()),
        }
    }
    fn into_order(self) -> Vec<String> {
        self.order.into_inner()
    }
}

impl crate::decoder::ColumnarSink for DiscoverySink {
    #[inline]
    fn begin_row(&mut self) {}
    #[inline]
    fn put_field(&mut self, name: &str, _value: crate::value::Value<'_>) {
        // Full-value fallback (if needs_value were true); keep for completeness.
        let mut seen = self.seen.borrow_mut();
        if seen.insert(Box::from(name)) {
            self.order.borrow_mut().push(name.to_string());
        }
    }
    #[inline]
    fn end_row(&mut self) {}
    #[inline]
    fn wants(&self, _name: &str) -> bool {
        true
    }
    #[inline]
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        // Locate-only path calls `resolve` without `put_field`; capture here.
        let mut seen = self.seen.borrow_mut();
        if seen.insert(Box::from(name)) {
            self.order.borrow_mut().push(name.to_string());
        }
        Some(name)
    }
    #[inline]
    fn needs_value(&self) -> bool {
        false
    }
    // needs_resolve defaults to true → scanner calls resolve() for each field.
    fn finish(&mut self) -> crate::Result<arrow::record_batch::RecordBatch> {
        Ok(arrow::record_batch::RecordBatch::new_empty(
            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        ))
    }
}

pub(crate) fn discover_schema<P: crate::decoder::RecordParser>(
    bytes: &[u8],
    parser: &P,
    plan: &crate::plan::ExecutionPlan,
    splitter: &dyn crate::decoder::Splitter,
) -> (FrozenSchema, Vec<String>) {
    let t0 = std::time::Instant::now();
    // Explicit schema already handled by caller; this is auto-discovery.
    let opts = DiscoveryOpts::default();

    let file_size = bytes.len() as u64;

    // Dynamic sizing: scale window count and size by file size
    let n = crate::schema::dynamic_window_count(file_size);
    let wbytes = crate::schema::dynamic_window_size(file_size);

    let order: Vec<String> = if file_size < opts.full_scan_threshold || n == 0 {
        // Small file or dynamic sizing says full scan
        let mut sink = DiscoverySink::new();
        let _ = parser.parse_chunk_generic(bytes, &mut sink);
        sink.into_order()
    } else {
        use rayon::prelude::*;
        // Parallelise windows: n×wbytes independent parses
        let per_window: Vec<Vec<String>> = (0..n)
            .into_par_iter()
            .map(|i| {
                let start = (file_size * i as u64 / n as u64) as usize;
                let end = (start + wbytes).min(bytes.len());
                if start >= end {
                    return Vec::new();
                }
                let mut sink = DiscoverySink::new();
                let slice = &bytes[start..end];
                let _ = parser.parse_chunk_generic(slice, &mut sink);
                sink.into_order()
            })
            .collect();
        // Merge in file order, deduplicating, so global order approximates file order.
        let mut seen = rustc_hash::FxHashSet::<String>::default();
        let mut merged = Vec::new();
        for mut v in per_window {
            for name in v.drain(..) {
                if seen.insert(name.clone()) {
                    merged.push(name);
                }
            }
        }
        // Always scan the tail to catch late-appearing columns
        if opts.always_scan_tail {
            let tail_start = bytes.len().saturating_sub(wbytes);
            let mut tail_sink = DiscoverySink::new();
            let _ = parser.parse_chunk_generic(&bytes[tail_start..], &mut tail_sink);
            for name in tail_sink.into_order() {
                if seen.insert(name.clone()) {
                    merged.push(name);
                }
            }
        }
        if merged.is_empty() && !bytes.is_empty() {
            let mut sink = DiscoverySink::new();
            let _ = parser.parse_chunk_generic(bytes, &mut sink);
            sink.into_order()
        } else {
            merged
        }
    };
    let elapsed = t0.elapsed().as_nanos() as u64;
    DISCOVERY_NS.store(elapsed, Ordering::Relaxed);
    let _ = splitter; // keep splitter in signature for future alignment
    let schema = if order.is_empty() {
        FrozenSchema::from_plan(&[], plan)
    } else {
        FrozenSchema::from_discovered(&order, plan)
    };
    (schema, order)
}

/// Public helper for batch workloads: discover the schema once and reuse.
/// Example:
/// ```rust,no_run
/// # use rypipe_core::{discover_schema_for_bytes, ExecutionPlan, RecordParser, Splitter};
/// fn example<S: Splitter, P: RecordParser>(bytes: &[u8], splitter: &S, parser: &P) {
///     let plan = ExecutionPlan::new();
///     let schema = discover_schema_for_bytes(bytes, splitter, parser, &plan);
///     let _reused = schema.clone();
/// }
/// ```
pub fn discover_schema_for_bytes<P: crate::decoder::RecordParser>(
    bytes: &[u8],
    splitter: &dyn crate::decoder::Splitter,
    parser: &P,
    plan: &crate::plan::ExecutionPlan,
) -> FrozenSchema {
    if !plan.schema_order.is_empty() {
        let names: Vec<&str> = plan.schema_order.iter().map(|s| s.as_str()).collect();
        return FrozenSchema::from_partial_plan(&names, plan);
    }
    let opts = DiscoveryOpts::default();
    let sig = crate::schema::layout_signature(bytes, &opts);
    {
        let cache = crate::schema::SCHEMA_CACHE
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(order) = cache.get(&sig) {
            crate::schema::SCHEMA_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
            return FrozenSchema::from_discovered(order, plan);
        }
    }
    crate::schema::SCHEMA_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    let (schema, order) = discover_schema(bytes, parser, plan, splitter);
    if !order.is_empty() {
        crate::schema::insert_schema_cache(sig, Arc::new(order));
    }
    schema
}
