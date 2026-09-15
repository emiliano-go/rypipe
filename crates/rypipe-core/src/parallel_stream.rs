//! Parallel streaming executor: multi-core parsing with bounded memory.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::bounded::MemoryBudget;
use crate::consumer::BatchConsumer;
use crate::decoder::{RecordParser, Splitter};
use crate::engine::TableBuilder;
use crate::input::InputBuffer;
use crate::plan::ExecutionPlan;
use crate::schema::{DiscoveryOpts, FrozenSchema};
use crate::Result;

mod discovery;

pub(crate) use discovery::discover_schema;
pub use discovery::{
    discover_schema_for_bytes, discovery_profile, reset_discovery_profile, ParallelStreamOpts,
};

/// Parallel streaming executor: parses chunks concurrently with bounded memory.
pub struct ParallelStreamingExecutor {
    budget: MemoryBudget,
    max_in_flight: usize,
}

type DispatchState = Arc<(Mutex<Option<usize>>, Condvar)>;

struct DispatchGuard(DispatchState);

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        *self.0 .0.lock().unwrap_or_else(|e| e.into_inner()) = None;
        self.0 .1.notify_all();
    }
}

impl ParallelStreamingExecutor {
    pub fn new(budget: MemoryBudget, max_in_flight: usize) -> Self {
        Self {
            budget,
            max_in_flight: max_in_flight.max(1),
        }
    }

    /// Stream a file in parallel, calling `consumer` per batch in order.
    #[allow(clippy::too_many_arguments)]
    pub fn run_stream<P, C>(
        &self,
        path: &Path,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        prefault: bool,
        opts: ParallelStreamOpts,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        C: BatchConsumer,
    {
        let input = InputBuffer::open(path, cfg!(feature = "mmap"), prefault)?;
        if input.is_empty() {
            return Ok(());
        }
        #[cfg(not(feature = "mmap"))]
        self.budget.share(4).check(input.len())?;
        #[cfg(feature = "mmap")]
        let opts = if let InputBuffer::Mmap(handle) = &input {
            let mut opts = opts;
            if opts.schema.is_none() && plan.schema_order.is_empty() {
                opts.schema = Some(discovery::discover_schema_for_file(
                    handle,
                    splitter,
                    &parser,
                    &plan,
                    self.budget,
                )?);
            }
            opts
        } else {
            self.budget.share(4).check(input.len())?;
            opts
        };
        // Share the InputBuffer across workers via Arc to avoid
        // the O(file_size × threads) to_vec() clone.
        let shared = std::sync::Arc::new(input);
        self.run_bytes_stream_shared(shared, splitter, parser, plan, opts, consumer)
    }

    /// Plan from shared input, then release file mappings before parsing.
    fn run_bytes_stream_shared<P, C>(
        &self,
        input: std::sync::Arc<InputBuffer>,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        opts: ParallelStreamOpts,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        C: BatchConsumer,
    {
        // Safety: the Arc keeps the InputBuffer alive for the duration.
        // We pass a dummy &[u8] to satisfy the signature; run_bytes_stream_core
        // ignores it when input is Some and uses input.as_slice() instead.
        let dummy: &[u8] = &[];
        self.run_bytes_stream_core(dummy, Some(input), splitter, parser, plan, opts, consumer)
    }

    /// Stream bytes in parallel.
    pub fn run_bytes_stream<P, C>(
        &self,
        bytes: &[u8],
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        opts: ParallelStreamOpts,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        C: BatchConsumer,
    {
        self.run_bytes_stream_core(bytes, None, splitter, parser, plan, opts, consumer)
    }

    /// Core implementation shared by `run_bytes_stream` and `run_bytes_stream_shared`.
    #[allow(clippy::too_many_arguments)]
    fn run_bytes_stream_core<P, C>(
        &self,
        bytes: &[u8],
        input: Option<std::sync::Arc<InputBuffer>>,
        splitter: &dyn Splitter,
        parser: P,
        plan: Arc<ExecutionPlan>,
        opts: ParallelStreamOpts,
        consumer: &mut C,
    ) -> Result<()>
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        C: BatchConsumer,
    {
        // Prefer the shared InputBuffer's slice over the passed-in bytes.
        let actual_bytes = match input {
            Some(ref inp) => inp.as_slice(),
            None => bytes,
        };
        if actual_bytes.is_empty() {
            return Ok(());
        }
        let n = opts.threads.max(1);
        let max_reorder = if opts.max_reorder > 0 {
            opts.max_reorder
        } else {
            n
        };
        // Frozen schema: gates correct ParquetWriter / StreamWriter usage.
        // Without it, batch 2 can have different column order (FieldG vs Text20
        // last) even when the set is identical, breaking `write_batch`.
        // If opts.schema is None, auto-discover:
        //  - explicit plan.schema_order → from_partial_plan (allows unknown columns)
        //  - else sampled discovery (16×2 MiB windows for >128 MiB, else full)
        let schema: Option<FrozenSchema> = match opts.schema {
            Some(s) => Some(s),
            None => {
                if !plan.schema_order.is_empty() {
                    let names: Vec<&str> = plan.schema_order.iter().map(|s| s.as_str()).collect();
                    Some(FrozenSchema::from_partial_plan(&names, &plan))
                } else {
                    let opts = DiscoveryOpts::default();
                    let sig = crate::schema::layout_signature(actual_bytes, &opts);
                    let cached = crate::schema::SCHEMA_CACHE
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .get(&sig)
                        .cloned();
                    let (schema, order) = if let Some(order) = cached {
                        crate::schema::SCHEMA_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
                        (FrozenSchema::from_discovered(&order, &plan), order)
                    } else {
                        crate::schema::SCHEMA_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
                        let (schema, order) =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                discover_schema(actual_bytes, &parser, &plan, splitter)
                            }))
                            .map_err(|_| {
                                crate::Error::Parser(
                                    "parser panicked during schema discovery".into(),
                                )
                            })?;
                        (schema, Arc::new(order))
                    };
                    if !order.is_empty() {
                        crate::schema::insert_schema_cache(sig, order);
                    }
                    Some(schema)
                }
            }
        };

        let bytes_per_row = splitter
            .estimate_bytes_per_row(&actual_bytes[..actual_bytes.len().min(65536)])
            .max(1);
        let slots = n
            .saturating_mul(3)
            .saturating_add(self.max_in_flight.saturating_mul(2))
            .saturating_add(max_reorder)
            .saturating_add(1);
        let allowance = self.budget.share(slots);
        let chunk_size = (self.budget.bytes() / n.saturating_mul(32))
            .max(bytes_per_row.saturating_mul(10))
            .max(if self.budget.is_strict() {
                1
            } else {
                64 * 1024
            });
        let num_chunks = actual_bytes.len().div_ceil(chunk_size).max(n).min(10000);
        let split_points = splitter.find_split_points(actual_bytes, num_chunks);
        let mut ranges = crate::decoder::split_points_to_ranges(&split_points, actual_bytes.len());
        if ranges.is_empty() {
            ranges.push(0..actual_bytes.len());
        }

        let chunks_with_seq: Vec<(usize, std::ops::Range<usize>)> =
            ranges.into_iter().enumerate().rev().collect();
        allowance.check(
            chunks_with_seq.capacity() * std::mem::size_of::<(usize, std::ops::Range<usize>)>(),
        )?;

        let (sender, receiver) = sync_channel(self.max_in_flight);
        let mut handles: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(n);
        let chunk_queue = std::sync::Arc::new(std::sync::Mutex::new(chunks_with_seq));
        let plan_arc = plan;
        let schema_arc = schema.map(std::sync::Arc::new);
        let dispatch = DispatchGuard(Arc::new((Mutex::new(Some(0)), Condvar::new())));
        let dispatch_window = max_reorder.saturating_add(1);
        let ordered = opts.ordered;
        let est_row = splitter
            .estimate_bytes_per_row(&actual_bytes[..actual_bytes.len().min(65536)])
            .max(512);

        let bytes_fallback = if input.is_none() {
            allowance.check(actual_bytes.len())?;
            Some(Arc::<[u8]>::from(actual_bytes))
        } else {
            None
        };
        let (input, file) = match input {
            #[cfg(feature = "mmap")]
            Some(input) if matches!(&*input, InputBuffer::Mmap(_)) => {
                let input = Arc::try_unwrap(input).map_err(|_| {
                    crate::Error::Plan("stream input still shared during planning".into())
                })?;
                let InputBuffer::Mmap(handle) = input else {
                    unreachable!()
                };
                drop(handle.mmap);
                (None, Some(Arc::new(std::sync::Mutex::new(handle.file))))
            }
            input => (input, None::<Arc<std::sync::Mutex<std::fs::File>>>),
        };
        for _ in 0..n {
            let queue = std::sync::Arc::clone(&chunk_queue);
            let sender_clone: SyncSender<(usize, Result<RecordBatch>)> = sender.clone();
            let parser_clone = parser.clone();
            let plan_clone = std::sync::Arc::clone(&plan_arc);
            let schema_clone = schema_arc.clone();
            let input_clone = input.clone();
            let fallback_clone = bytes_fallback.clone();
            let file_clone = file.clone();
            let dispatch_clone = dispatch.0.clone();
            let handle = thread::spawn(move || -> Result<()> {
                let bytes_ref: &[u8] = match input_clone {
                    Some(ref inp) => inp.as_slice(),
                    None => fallback_clone.as_deref().unwrap_or(&[]),
                };
                let mut chunk_buffer = Vec::new();
                loop {
                    let next = {
                        let mut q = queue.lock().unwrap_or_else(|e| e.into_inner());
                        q.pop()
                    };
                    let Some((seq, range)) = next else { break };
                    if ordered {
                        let (next, ready) = &*dispatch_clone;
                        let next = ready
                            .wait_while(next.lock().unwrap_or_else(|e| e.into_inner()), |next| {
                                next.is_some_and(|next| seq >= next.saturating_add(dispatch_window))
                            })
                            .unwrap_or_else(|e| e.into_inner());
                        if next.is_none() {
                            break;
                        }
                    }
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || -> Result<RecordBatch> {
                            allowance.check(range.len())?;
                            let chunk_bytes = if let Some(file) = &file_clone {
                                chunk_buffer.resize(range.len(), 0);
                                allowance.check(chunk_buffer.capacity())?;
                                let mut file = file.lock().unwrap_or_else(|e| e.into_inner());
                                file.seek(SeekFrom::Start(range.start as u64))?;
                                file.read_exact(&mut chunk_buffer)?;
                                chunk_buffer.as_slice()
                            } else {
                                &bytes_ref[range.start..range.end]
                            };
                            let mut builder = TableBuilder::with_plan(
                                (chunk_bytes.len() / est_row).max(64),
                                plan_clone.clone(),
                            );
                            builder.set_memory_budget(allowance)?;
                            // If schema is provided, pre-size columns from it.
                            if let Some(ref schema) = schema_clone {
                                builder.ensure_schema(schema)?;
                                builder.check_memory_budget()?;
                            }
                            parser_clone.validate(chunk_bytes)?;
                            parser_clone.parse_chunk_generic(chunk_bytes, &mut builder)?;
                            let batch = builder.finish()?;
                            allowance.check(batch.get_array_memory_size())?;
                            Ok(batch)
                        },
                    ))
                    .unwrap_or_else(|_| {
                        Err(crate::Error::Parser(
                            "worker panicked during parallel parse".into(),
                        ))
                    });
                    let failed = result.is_err();
                    if sender_clone.send((seq, result)).is_err() || failed {
                        break;
                    }
                }
                Ok(())
            });
            handles.push(handle);
        }
        drop(sender);

        // Coordinator: order by seq and deliver.
        // If `ordered`, buffer out-of-order batches until the next-in-sequence arrives.
        // If unordered, deliver immediately.
        let mut pending: BTreeMap<usize, RecordBatch> = BTreeMap::new();
        let mut next_seq = 0usize;
        let mut reorder_bytes: usize = 0;
        let reorder_limit = max_reorder.saturating_mul(allowance.bytes());
        let mut result = (|| -> Result<()> {
            for (seq, res) in receiver {
                let batch = res?;
                if opts.ordered {
                    if seq == next_seq {
                        consumer.consume(batch)?;
                        next_seq += 1;
                        while let Some(b) = pending.remove(&next_seq) {
                            reorder_bytes -= b.get_array_memory_size();
                            consumer.consume(b)?;
                            next_seq += 1;
                        }
                        *dispatch.0 .0.lock().unwrap_or_else(|e| e.into_inner()) = Some(next_seq);
                        dispatch.0 .1.notify_all();
                    } else {
                        reorder_bytes = reorder_bytes.saturating_add(batch.get_array_memory_size());
                        if self.budget.is_strict() && reorder_bytes > reorder_limit {
                            return Err(crate::Error::Memory {
                                used: reorder_bytes,
                                limit: reorder_limit,
                            });
                        }
                        pending.insert(seq, batch);
                    }
                } else {
                    consumer.consume(batch)?;
                }
            }
            Ok(())
        })();
        drop(dispatch);
        for h in handles {
            let joined = h
                .join()
                .map_err(|_| {
                    crate::Error::Parser(
                        "worker panicked during parallel parse. \
                     This usually indicates a bug in the parser (e.g., returning \
                     Cow::Borrowed that outlives the input chunk, or an unwrap() \
                     on None/Err during parsing). Check your RecordParser::parse_chunk \
                     implementation for incorrect lifetime handling or missing error checks."
                            .to_string(),
                    )
                })
                .and_then(|r| r);
            if result.is_ok() {
                result = joined;
            }
        }
        result
    }
}

/// Iterator wrapper for parallel streaming (pull-based).
pub struct ParallelStreamingBatchIterator {
    receiver: Receiver<Result<RecordBatch>>,
    handle: Option<JoinHandle<Result<()>>>,
    done: bool,
}

impl ParallelStreamingBatchIterator {
    pub fn new<P, S>(
        path: PathBuf,
        splitter: S,
        parser: P,
        plan: Arc<ExecutionPlan>,
        budget: MemoryBudget,
        prefault: bool,
        opts: ParallelStreamOpts,
    ) -> Self
    where
        P: RecordParser + Clone + Send + Sync + 'static,
        S: Splitter + Clone + Send + Sync + 'static,
    {
        let num_threads = opts.threads;
        let max_in_flight = 2 * num_threads;
        let (sender, receiver) = sync_channel(max_in_flight);
        let handle = thread::spawn(move || {
            let exec = ParallelStreamingExecutor::new(budget, max_in_flight);
            let mut consumer = ChannelConsumer {
                sender: sender.clone(),
            };
            let res = exec.run_stream(
                &path,
                &splitter,
                parser,
                plan,
                prefault,
                opts,
                &mut consumer,
            );
            if let Err(e) = res {
                let _ = sender.send(Err(e));
            }
            Ok(())
        });
        Self {
            receiver,
            handle: Some(handle),
            done: false,
        }
    }
}

struct ChannelConsumer {
    sender: SyncSender<Result<RecordBatch>>,
}
impl BatchConsumer for ChannelConsumer {
    fn consume(&mut self, batch: RecordBatch) -> Result<()> {
        self.sender
            .send(Ok(batch))
            .map_err(|_| crate::Error::Parser("stream consumer disconnected".into()))
    }
}

impl Iterator for ParallelStreamingBatchIterator {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.receiver.recv() {
            Ok(Ok(batch)) => Some(Ok(batch)),
            Ok(Err(e)) => {
                self.done = true;
                Some(Err(e))
            }
            Err(_) => {
                self.done = true;
                if let Some(h) = self.handle.take() {
                    if let Err(p) = h.join() {
                        let msg = if let Some(s) = p.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else if let Some(s) = p.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "worker panicked".to_string()
                        };
                        return Some(Err(crate::Error::Parser(format!(
                            "parallel streaming worker panicked: {msg}. \
                             This usually indicates a bug in the parser (e.g., returning \
                             Cow::Borrowed that outlives the input chunk, or an unwrap() \
                             on None/Err during parsing). Check your RecordParser::parse_chunk \
                             implementation for incorrect lifetime handling or missing error checks."
                        ))));
                    }
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    #[test]
    fn channel_consumer_reports_disconnect() {
        let (sender, receiver) = sync_channel(1);
        drop(receiver);
        let mut consumer = ChannelConsumer { sender };
        let batch = RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()));
        assert!(consumer.consume(batch).is_err());
    }
    use crate::decoder::{ColumnarSink, RecordParser, Splitter};
    use crate::plan::ExecutionPlan;
    use crate::schema::clear_schema_cache;
    use crate::value::Value;
    use arrow::array::{Array, AsArray};

    /// Minimal parser: newline-separated rows of `key=value\t...` tokens.
    #[derive(Clone, Debug, Default)]
    struct NameValueParser;

    impl RecordParser for NameValueParser {
        fn validate(&self, _bytes: &[u8]) -> Result<()> {
            Ok(())
        }

        fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
            let text = std::str::from_utf8(bytes).map_err(|e| crate::Error::Plan(e.to_string()))?;
            for line in text.lines() {
                if line.is_empty() {
                    continue;
                }
                sink.begin_row();
                for token in line.split('\t') {
                    if let Some((k, v)) = token.split_once('=') {
                        sink.resolve_and_put(k, Value::Str(Cow::Borrowed(v)));
                    }
                }
                sink.end_row();
            }
            Ok(())
        }
    }

    /// Trivial splitter: one chunk covering the whole input.
    struct NullSplitter;

    impl Splitter for NullSplitter {
        fn next_record_start(&self, _bytes: &[u8], _from: usize) -> Option<usize> {
            None
        }

        fn find_split_points(&self, bytes: &[u8], _max_chunks: usize) -> Vec<usize> {
            vec![0, bytes.len()]
        }

        fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
            let n = sample.iter().filter(|&&b| b == b'\n').count().max(1);
            sample.len() / n
        }
    }

    struct TwoChunkSplitter;
    impl Splitter for TwoChunkSplitter {
        fn next_record_start(&self, _bytes: &[u8], _from: usize) -> Option<usize> {
            None
        }
        fn find_split_points(&self, bytes: &[u8], _max_chunks: usize) -> Vec<usize> {
            vec![0, 4.min(bytes.len()), bytes.len()]
        }
        fn estimate_bytes_per_row(&self, _sample: &[u8]) -> usize {
            5
        }
    }

    struct VecConsumer(Vec<RecordBatch>);
    impl BatchConsumer for VecConsumer {
        fn consume(&mut self, batch: RecordBatch) -> Result<()> {
            self.0.push(batch);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct DelayedParser;
    impl RecordParser for DelayedParser {
        fn validate(&self, bytes: &[u8]) -> Result<()> {
            NameValueParser.validate(bytes)
        }
        fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
            if bytes.starts_with(b"a=1") {
                std::thread::sleep(std::time::Duration::from_millis(30));
            }
            NameValueParser.parse_chunk(bytes, sink)
        }
    }

    #[test]
    fn ordered_soft_stream_tolerates_oversized_pending_batch() {
        let exec = ParallelStreamingExecutor::new(MemoryBudget::new(1), 2);
        let mut consumer = VecConsumer(Vec::new());
        let opts = ParallelStreamOpts {
            threads: 2,
            max_reorder: 1,
            ..Default::default()
        };
        let result = exec.run_bytes_stream(
            b"a=1\nb=2\n",
            &TwoChunkSplitter,
            DelayedParser,
            Arc::new(ExecutionPlan::new()),
            opts,
            &mut consumer,
        );
        result.unwrap();
        assert_eq!(
            consumer.0.iter().map(RecordBatch::num_rows).sum::<usize>(),
            2
        );
        assert_eq!(
            consumer.0[0]
                .column_by_name("a")
                .unwrap()
                .as_string::<i32>()
                .value(0),
            "1"
        );
        assert_eq!(
            consumer.0[1]
                .column_by_name("b")
                .unwrap()
                .as_string::<i32>()
                .value(0),
            "2"
        );
    }

    fn names(schema: &FrozenSchema) -> Vec<&str> {
        schema.column_names().iter().map(|s| s.as_ref()).collect()
    }

    #[test]
    fn schema_cache_hits_misses_and_plan_changes() {
        clear_schema_cache();
        let splitter = NullSplitter;
        let parser = NameValueParser;

        // First layout: miss.
        let bytes = b"a=1\tb=2\na=3\tb=4\n";
        let plan = ExecutionPlan::new();
        let s1 = discover_schema_for_bytes(bytes, &splitter, &parser, &plan);
        assert_eq!(names(&s1), vec!["a", "b"]);

        // Same layout: hit (cached).
        let s2 = discover_schema_for_bytes(bytes, &splitter, &parser, &plan);
        assert_eq!(names(&s2), vec!["a", "b"]);

        // Different layout: new discovery.
        let bytes2 = b"c=1\td=2\n";
        let s3 = discover_schema_for_bytes(bytes2, &splitter, &parser, &plan);
        assert_eq!(names(&s3), vec!["c", "d"]);

        // Same layout as the first parse but with a different plan:
        // cache hit, but the applied schema differs.
        let mut renamed = ExecutionPlan::new();
        renamed.field_map.insert("a".to_string(), "x".to_string());
        let s4 = discover_schema_for_bytes(bytes, &splitter, &parser, &renamed);
        assert_eq!(names(&s4), vec!["x", "b"]);

        clear_schema_cache();
    }

    #[test]
    fn tail_scan_catches_late_columns() {
        // Create data where column "late" only appears in the last few rows.
        // With 16 windows of 2 MiB each, the last window starts at ~94%.
        // Column "late" appears only in the last ~5% of the data.
        let mut data = Vec::new();
        // First 90%: only columns "a" and "b"
        for i in 0..900 {
            data.extend_from_slice(format!("a={i}\tb=val{i}\n").as_bytes());
        }
        // Last 10%: introduce column "late"
        for i in 900..1000 {
            data.extend_from_slice(format!("a={i}\tb=val{i}\tlate=new{i}\n").as_bytes());
        }

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);

        // Column "late" must be discovered
        let cols = names(&schema);
        assert!(cols.contains(&"a"), "column 'a' missing");
        assert!(cols.contains(&"b"), "column 'b' missing");
        assert!(
            cols.contains(&"late"),
            "column 'late' missing from tail scan"
        );

        // Verify all values are preserved by parsing the full data
        let mut builder = crate::engine::TableBuilder::with_plan(1024, Arc::new(plan.clone()));
        builder.ensure_schema(&schema).unwrap();
        parser.parse_chunk(&data, &mut builder).unwrap();
        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 1000);
        assert_eq!(batch.num_columns(), 3);

        // Check that the "late" column has values in the last 100 rows
        let late_col = batch.column_by_name("late").expect("late column missing");
        let late_arr = late_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("late column not string array");
        // First 900 rows should be null for "late"
        for i in 0..900 {
            assert!(
                late_arr.is_null(i),
                "row {i}: expected null for 'late', got {:?}",
                late_arr.value(i)
            );
        }
        // Last 100 rows should have values
        for i in 900..1000 {
            assert!(
                !late_arr.is_null(i),
                "row {i}: expected value for 'late', got null"
            );
            assert!(
                late_arr.value(i).starts_with("new"),
                "row {i}: expected 'new{i}', got {:?}",
                late_arr.value(i)
            );
        }
    }

    #[test]
    fn all_columns_discovered_no_data_loss() {
        // Test that all columns from a file are discovered, even with sparse data
        let mut data = Vec::new();
        // Mix of columns across rows
        let rows: Vec<&[u8]> = vec![
            b"a=1\tb=2\tc=3\n",
            b"b=5\td=6\n",
            b"a=7\tc=8\te=9\n",
            b"f=10\n",
            b"a=11\tb=12\tc=13\td=14\te=15\tf=16\n",
        ];
        for row in &rows {
            data.extend_from_slice(row);
        }

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);

        // All 6 columns must be discovered
        let cols = names(&schema);
        assert_eq!(cols.len(), 6, "expected 6 columns, got {:?}", cols);
        assert!(cols.contains(&"a"));
        assert!(cols.contains(&"b"));
        assert!(cols.contains(&"c"));
        assert!(cols.contains(&"d"));
        assert!(cols.contains(&"e"));
        assert!(cols.contains(&"f"));

        // Parse all data and verify values
        let mut builder = crate::engine::TableBuilder::with_plan(1024, Arc::new(plan.clone()));
        builder.ensure_schema(&schema).unwrap();
        parser.parse_chunk(&data, &mut builder).unwrap();
        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 6);

        // Verify specific values
        let a_col = batch.column_by_name("a").unwrap();
        let a_arr = a_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(a_arr.value(0), "1");
        assert_eq!(a_arr.value(2), "7");
        assert_eq!(a_arr.value(4), "11");
    }

    #[test]
    fn dynamic_sizing_discovers_same_columns() {
        // Verify that dynamic window sizing discovers the same columns
        // as the fixed 16-window approach
        let mut data = Vec::new();
        // Create data with columns spread across the file
        for i in 0..1000 {
            if i < 100 {
                // First 10%: columns a, b
                data.extend_from_slice(format!("a={i}\tb={i}\n").as_bytes());
            } else if i < 500 {
                // Middle 40%: columns a, b, c
                data.extend_from_slice(format!("a={i}\tb={i}\tc={i}\n").as_bytes());
            } else if i < 900 {
                // Next 40%: columns a, b, c, d
                data.extend_from_slice(format!("a={i}\tb={i}\tc={i}\td={i}\n").as_bytes());
            } else {
                // Last 10%: columns a, b, c, d, e (late column)
                data.extend_from_slice(format!("a={i}\tb={i}\tc={i}\td={i}\te={i}\n").as_bytes());
            }
        }

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);

        // All 5 columns must be discovered
        let cols = names(&schema);
        assert_eq!(cols.len(), 5, "expected 5 columns, got {:?}", cols);
        assert!(cols.contains(&"a"));
        assert!(cols.contains(&"b"));
        assert!(cols.contains(&"c"));
        assert!(cols.contains(&"d"));
        assert!(cols.contains(&"e"));

        // Parse and verify
        let mut builder = crate::engine::TableBuilder::with_plan(1024, Arc::new(plan.clone()));
        builder.ensure_schema(&schema).unwrap();
        parser.parse_chunk(&data, &mut builder).unwrap();
        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 1000);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn column_order_consistent_across_file_sizes() {
        // Verify that column order is consistent regardless of file size
        // This ensures no data reordering issues
        let make_data = |n: usize| -> Vec<u8> {
            let mut data = Vec::new();
            for i in 0..n {
                data.extend_from_slice(format!("x={i}\ty={i}\tz={i}\n").as_bytes());
            }
            data
        };

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();

        // Test with different file sizes
        let sizes = vec![10, 100, 1000];
        let mut first_order: Option<Vec<String>> = None;

        for size in sizes {
            let data = make_data(size);
            let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);
            let cols: Vec<String> = schema
                .column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();

            if let Some(ref order) = first_order {
                assert_eq!(
                    &cols, order,
                    "column order changed for size {size}: {cols:?} vs {order:?}"
                );
            } else {
                first_order = Some(cols);
            }
        }
    }

    #[test]
    fn esoteric_sparse_schema_one_column_per_row() {
        // Each row has a unique column name - extremely sparse
        let mut data = Vec::new();
        for i in 0..100 {
            data.extend_from_slice(format!("col_{i}={i}\n").as_bytes());
        }

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);

        // All 100 columns should be discovered
        let cols = names(&schema);
        assert_eq!(
            cols.len(),
            100,
            "expected 100 columns, got {:?}",
            cols.len()
        );
    }

    #[test]
    fn esoteric_empty_rows() {
        // File with many empty rows interspersed
        let mut data = Vec::new();
        for i in 0..50 {
            data.extend_from_slice(format!("a={i}\n").as_bytes());
            data.extend_from_slice(b"\n"); // empty row
            data.extend_from_slice(b"\n"); // another empty row
            data.extend_from_slice(format!("b={i}\n").as_bytes());
        }

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(&data, &splitter, &parser, &plan);

        let cols = names(&schema);
        assert!(cols.contains(&"a"), "column 'a' missing");
        assert!(cols.contains(&"b"), "column 'b' missing");
    }

    #[test]
    fn esoteric_single_character_columns() {
        // Very short column names
        let data = b"a=1\tb=2\tc=3\nd=4\te=5\n";

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(data, &splitter, &parser, &plan);

        let cols = names(&schema);
        assert_eq!(cols.len(), 5);
        assert!(cols.contains(&"a"));
        assert!(cols.contains(&"e"));
    }

    #[test]
    fn esoteric_unicode_column_names() {
        // Unicode characters in column names
        let data = "名前=Alice\t年齢=30\n名前=Bob\t年齢=25\n".as_bytes();

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(data, &splitter, &parser, &plan);

        let cols = names(&schema);
        assert_eq!(cols.len(), 2);
        assert!(cols.contains(&"名前"));
        assert!(cols.contains(&"年齢"));
    }

    #[test]
    fn esoteric_very_long_column_names() {
        // Very long column names (100+ chars)
        let long_name = "a".repeat(200);
        let data = format!("{long_name}=1\tb=2\n{long_name}=3\tc=4\n");

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(data.as_bytes(), &splitter, &parser, &plan);

        let cols = names(&schema);
        assert_eq!(cols.len(), 3);
        assert!(cols.contains(&long_name.as_str()));
    }

    #[test]
    fn esoteric_duplicate_columns_in_row() {
        // Same column appears multiple times in a row
        let data = b"a=1\ta=2\ta=3\tb=4\n";

        let splitter = NullSplitter;
        let parser = NameValueParser;
        let plan = ExecutionPlan::new();
        let schema = discover_schema_for_bytes(data, &splitter, &parser, &plan);

        let cols = names(&schema);
        assert_eq!(cols.len(), 2, "expected 2 unique columns, got {:?}", cols);
        assert!(cols.contains(&"a"));
        assert!(cols.contains(&"b"));
    }
}
