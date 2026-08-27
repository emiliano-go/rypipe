Read this repo and plan to separate the core engine.

Yes. I looked at v1.1.0 specifically, and it actually strengthens my recommendation to extract the engine now rather than later. The release hardened exactly the areas that are important to preserve as the generic core: differential testing across engines, fusion property tests, fuzzing, typed planning/merge errors, bounded/parallel correctness, and Arrow compatibility. 
G
GitHub

I would not extract “the Rust code” wholesale. I would extract a set of carefully defined layers.

The boundary I'd aim for

Think of the new project as:

                    FORMAT ADAPTERS
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
        XML            JSON            CSV
          │              │              │
          └──────────────┼──────────────┘
                         ▼
                 ┌───────────────┐
                 │ Decoder API   │
                 └───────┬───────┘
                         ▼
                 ┌───────────────┐
                 │ Execution     │
                 │ Engine        │
                 │               │
                 │ BuildPlan     │
                 │ ColumnBuilder │
                 │ filtering     │
                 │ casting       │
                 │ projection    │
                 │ dictionaries  │
                 └───────┬───────┘
                         ▼
                       Arrow


The upper half is format-specific.

The lower half is your reusable asset.

1. Extract BuildPlan first

This is probably the most important abstraction.

Your current plan contains:

field_map
drop_fields
field_types
dictionary_columns
filter
schema_order
auto_dict


Almost all of that is format-independent.

I'd turn it into something like:

pub struct ExecutionPlan {
    pub projection: Projection,
    pub renames: RenamePlan,
    pub casts: CastPlan,
    pub filter: Option<Predicate>,
    pub dictionaries: DictionaryPlan,
    pub schema: SchemaPlan,
}


I'd actually rename BuildPlan → ExecutionPlan or IngestPlan in the new project.

Why?

Because once you support JSON/CSV/XML, you're no longer describing how to build an XML table. You're describing what should happen to records coming from any decoder.

2. Extract ColumnBuilder

This is absolutely core.

Your current:

ColumnBuilder
 ├── String
 ├── Int64
 ├── Float64
 ├── Boolean
 └── Dictionary


should become completely unaware of XML.

It should accept logical values and produce Arrow-compatible arrays.

I'd probably make this a major subsystem:

columnar/
├── string.rs
├── primitive.rs
├── boolean.rs
├── dictionary.rs
├── validity.rs
└── builder.rs


This is reusable for everything.

3. Extract StrColumn

Definitely.

Your arena + offsets + validity representation is one of the strongest pieces of the current implementation.

It should have absolutely no concept of:

Field
Text
Section
Details
Crystal Reports
XML


It just knows:

append(value)
append_null()
pop()
len()
finish()


That becomes a generic high-performance string column builder.

4. Extract type conversion

Your current lexical-based:

string → int64
string → float64
string → bool


logic belongs in the core.

But I'd make it slightly more abstract:

Value
 ├── String
 ├── Int64
 ├── Float64
 ├── Boolean
 └── Null


The decoder can provide strings, while the execution plan determines whether the destination is typed.

This is important because CSV, XML, JSON and HTML all have different notions of native types.

For example JSON can give you:

{"amount": 123.45}


without requiring string parsing.

Your engine shouldn't unnecessarily turn that into:

"123.45"


and then parse it back.

So the generic value model should support typed input values.

5. Extract predicates

Your current filter machinery is reusable:

field == constant
field != constant

field_a > field_b
field_a < field_b
...


I'd make:

Predicate


a first-class core object.

Eventually:

Predicate
├── Eq
├── Ne
├── Lt
├── Le
├── Gt
├── Ge
├── And
├── Or
└── Not


You don't necessarily need all of those immediately.

But I'd design the abstraction so you can add them without rewriting the decoder.

6. Extract the projection/rename/drop system

These are really projection operations, not XML operations.

Your:

RenameFields
DropFields
schema_order


should become generic execution-plan operations.

I'd consider calling the abstraction:

Projection


rather than having three completely independent concepts internally.

For example:

Projection:
    source "customer_id" → output "customer"
    source "amount"      → output "amount"
    drop "internal_id"


That is effectively a schema projection.

7. Extract dictionary encoding

Absolutely.

Both:

dictionary_columns
auto_dict


belong in the core.

They have nothing to do with XML.

In fact, CSV and JSON are likely to benefit from dictionary encoding more often than XML.

8. Extract Arrow export

Definitely.

Your current:

ColumnBuilder
      ↓
RecordBatch
      ↓
Arrow C Data Interface
      ↓
Python


is a core subsystem.

I'd isolate:

arrow/
├── record_batch.rs
├── schema.rs
├── ffi.rs
└── table.rs


And make Python just one consumer.

This is important because your future project shouldn't be conceptually:

Rust engine for Python.

It should be:

Rust engine that produces Arrow, with Python bindings.

Then you can eventually expose it to:

Python
Rust
maybe Node
maybe R
maybe C/C++

without redesigning the engine.

9. Extract the parallel execution machinery

Yes, but not splitter.rs as-is.

Separate:

Generic
parallel executor
chunk scheduling
Rayon
worker lifecycle
batch reduction
ordered/unordered execution

Format-specific
"where is it safe to split this input?"


This distinction is crucial.

For CSV:

newline


might be a split boundary, except for quoted newlines.

For JSONL:

newline


is straightforward.

For XML:

<Details>


is your boundary.

For JSON arrays:

{ ... }


requires structural parsing.

So I'd create:

trait Splitter {
    fn split(
        &self,
        input: &[u8],
        target_chunks: usize,
    ) -> Result<Vec<Range<usize>>>;
}


The parallel engine doesn't care how the ranges were obtained.

10. Extract bounded execution

Definitely.

This is one of your most valuable generic features.

The current:

memory="500MB"


mechanism should become something like:

MemoryBudget
BoundedExecutor
BatchSizer


But the sampling strategy should belong to the decoder.

For example:

XML → estimate rows from row-tag density
CSV → estimate records from newline density
JSON → inspect array structure


The generic engine needs only:

"give me another batch of approximately N rows"

11. Extract the batch execution / Volcano layer

Definitely.

This:

RecordBatch
    ↓
operator
    ↓
operator
    ↓
operator
    ↓
sink


is entirely format-independent.

I'd make it a proper core subsystem.

Something like:

execution/
├── operator.rs
├── pipeline.rs
├── filter.rs
├── projection.rs
├── cast.rs
├── map.rs
└── sink.rs


Your existing Layer B is basically already this.

12. Extract fusion — but redesign its interface

Your current _plan_kwargs() is Python-specific:

stage._plan_kwargs()


Don't bring that abstraction into the Rust core.

Instead, define a language-independent logical plan.

Conceptually:

Source
  ↓
Rename
  ↓
Cast
  ↓
Filter
  ↓
Project
  ↓
Sink


Then the optimizer determines:

Can this operation execute in decoder?
Can it execute in column builder?
Can it execute as Arrow?
Does it require Python?


That's the generalized version of what you're already doing.

13. Do NOT extract RowParser

This is where I'd draw a hard line.

Your current:

RowParser
quick-xml
<Field>
<Text>
<Section>


is Crystal-specific.

Leave it in crxml.

The generic engine should never know that <Field> exists.

14. Do NOT extract splitter.rs wholesale

Same reason.

Keep:

CrystalXmlSplitter


inside crxml.

Extract only the splitter interface + parallel scheduler.

15. Do NOT extract the Crystal XML flattening semantics

This:

<Field FieldName="X">
<Text Name="Y">
<Section SectionNumber="N">


is your adapter.

I'd make it produce generic records.

Something conceptually like:

trait RecordDecoder {
    fn next_batch(
        &mut self,
        sink: &mut dyn RecordSink,
    ) -> Result<usize>;
}


Then Crystal XML implements it.

16. I would also extract the mmap abstraction

Your mmap lifecycle is more reusable than it initially appears.

Things like:

MmapHandle
prefault
MADV_WILLNEED
MADV_SEQUENTIAL
unmap_now


belong in a generic input layer.

But I'd call the abstraction something broader:

InputBuffer


because eventually not everything will be a file.

You could have:

FileInput
MmapInput
BytesInput
ReaderInput


The decoder gets bytes/ranges without knowing how they arrived.

17. Keep Python-specific code out of the core

This is important.

Your new repo should ideally have:

generic-engine/
├── engine-core/       ← Rust
├── engine-python/    ← PyO3
└── adapters/


The Rust core should not know:

PyObject
PyList
PyDict
Pandas
Polars
Python callable


Arrow is the boundary.

That's what makes the project potentially much bigger than Python.

What I would end up with

Something approximately like:

engine/
│
├── core/
│   ├── plan/
│   │   ├── execution_plan.rs
│   │   ├── projection.rs
│   │   ├── predicate.rs
│   │   └── cast.rs
│   │
│   ├── columnar/
│   │   ├── builder.rs
│   │   ├── string.rs
│   │   ├── primitive.rs
│   │   ├── dictionary.rs
│   │   └── validity.rs
│   │
│   ├── execution/
│   │   ├── operator.rs
│   │   ├── pipeline.rs
│   │   ├── batch.rs
│   │   └── optimizer.rs
│   │
│   ├── parallel/
│   │   ├── executor.rs
│   │   ├── scheduler.rs
│   │   └── splitter.rs   ← trait only
│   │
│   ├── memory/
│   │   ├── input.rs
│   │   ├── mmap.rs
│   │   └── budget.rs
│   │
│   └── arrow/
│       ├── export.rs
│       └── ffi.rs
│
├── decoder-api/
│   ├── decoder.rs
│   ├── record.rs
│   └── splitter.rs
│
└── python/
    ├── source.py
    ├── pipeline.py
    └── stages/


Then crxml becomes roughly:

crxml/
├── crystal_decoder.rs
├── crystal_splitter.rs
├── crystal_grammar.rs
└── python/
    └── CrystalXMLSource

One major change I'd make during extraction

Don't make the generic decoder emit dict[str, str].

That's the most important thing.

If you do:

XML → dict
JSON → dict
CSV → dict
     ↓
generic engine


you've already thrown away much of what makes crxml fast.

Instead:

                    RecordDecoder
                         │
                 Field/Value events
                         │
                         ▼
                  ExecutionPlan
                         │
                         ▼
                  ColumnBuilders
                         │
                         ▼
                       Arrow


The decoder should be able to say something like:

field = "amount"
value = Float64(123.45)


or:

field = "customer"
value = String("Alice")


without constructing a Python object or necessarily allocating a Rust String.

That gives JSON an especially nice advantage: native JSON numbers can remain numbers all the way through the pipeline.

And v1.1.0 gives you a useful extraction checkpoint

The release is now at 233 tests, with differential testing of stream/columnar/parallel/bounded paths against an independent ElementTree oracle, randomized fusion property tests, and a full-parser fuzz target. It also fixed several subtle semantic mismatches between execution paths. 
G
GitHub

That means I'd freeze crxml around its current semantics and use v1.1.0 as the reference implementation while extracting.

In particular, don't refactor correctness and generalization simultaneously.

Do this:

Phase 1: Freeze crxml 1.1 semantics.

Phase 2: Extract generic core with Crystal XML as its first adapter.

Phase 3: Make the new engine produce byte-for-byte/semantic-equivalent results to crxml.

Phase 4: Move the Crystal-specific code in crxml to consume the new engine.

Phase 5: Add CSV.

Phase 6: Add NDJSON.

Phase 7: Generic XML.

Phase 8: JSON/HTML/etc.

That gives you an extraordinarily useful invariant:

The first external adapter is the existing crxml implementation, so the new engine has a real, extremely demanding compatibility test from day one.

And given that v1.1.0 just added exactly the differential/property/fuzz infrastructure needed for this kind of refactoring, now is probably the best point in the project's life to do the extraction. 
G
GitHub

crxml v1.1.0 release crxml repository

G
Sources 
Thought: 4.2s
The user has pasted what appears to be a detailed analysis/recommendation (possibly from another AI or their own notes) about extracting a generic engine core from their crxml project. They want me to read the repo and plan the separation of the core engine.

I'm in plan mode - READ ONLY. I need to:
1. Read the repo to understand its structure
2. Understand the codebase deeply enough to validate/refine the plan
3. Present a well-researched plan
4. Possibly ask clarifying questions

Let me start by exploring the repository structure. I should use parallel tool calls to explore efficiently. Let me first look at the directory structure, then dive into key files like Cargo.toml, README, and the source files mentioned (BuildPlan, ColumnBuilder, StrColumn, splitter.rs, etc.).

Let me launch exploration in parallel:
- Read the root directory
- Read Cargo.toml
- Look at src structure

Then based on that, dive into specific files.
→Read . 
✱Glob "**/*.rs" (5 matches)
✱Glob "*.{toml,md,txt,cfg}" (31 matches)
Thought: 2.4s
The Rust code is smaller than I expected - only 3 source files in src/crxml_core. Let me look at the full structure of src, the Python package layout, and read key files. Let me explore in parallel:

1. Read src directory structure
2. Read Cargo.toml files
3. Read lib.rs, columnar.rs, splitter.rs
4. Read pyproject.toml
5. Look at the python package

Let me do this efficiently.
$ ls -R src tests | head -100
src:
crxml
crxml_core

src/crxml:
batchpipe.py
_crxml_core.cpython-312-x86_64-linux-gnu.so
_crxml_core.cpython-314-x86_64-linux-gnu.so
fusion.py
__init__.py
…
Click to expand
→Read src/crxml_core/Cargo.toml 
→Read pyproject.toml 
Thought: 855ms
Now let me look at the Rust source files. There are only 3: lib.rs, columnar.rs, splitter.rs. Let me check their sizes first and read them.
$ wc -l src/crxml_core/src/*.rs && ls tests/ | head -60 && wc -l tests/*.py 2>/dev/null | tail -5
2085 src/crxml_core/src/columnar.rs
  1196 src/crxml_core/src/lib.rs
   304 src/crxml_core/src/splitter.rs
  3585 total
conftest.py
__pycache__
test_batchpipe.py
test_columnar_export.py
test_differential.py
test_fusion_props.py
…
Click to expand
Thought: 571ms
The Rust core is only 3585 lines total. Let me read all three Rust files to understand the structure deeply. I'll read them in parallel.
→Read src/crxml_core/src/lib.rs 
Thought: 429ms
Now let me read the columnar.rs and splitter.rs files.
→Read src/crxml_core/src/columnar.rs 
Thought: 738ms
Let me continue reading columnar.rs from line 1181, and also read splitter.rs.
→Read src/crxml_core/src/columnar.rs [offset=1181]
→Read src/crxml_core/src/splitter.rs 
Thought: 1.3s
Now I have a good understanding of the Rust core. Let me look at the Python layer to understand the full picture: source.py, pipeline.py, fusion.py, parallel.py, batchpipe.py, sinks.py, and the stages. Let me also check the architecture docs.
→Read src/crxml/source.py 
→Read docs/architecture.md 
Thought: 4.3s
I now have a very good picture of the codebase. Let me quickly check the remaining Python files (pipeline.py, fusion.py, batchpipe.py, parallel.py, stages) briefly, plus the tests to understand what the compatibility surface looks like. Actually, I have a lot of context already from architecture.md which describes these in detail. Let me quickly scan the remaining Python files to confirm sizes and any details, then think about the plan.

Let me also look at the fuzz targets and scripts directory to understand CI, and check the git log briefly. Actually let me be efficient — I have enough for planning. Let me do one quick pass over the remaining Python files with line counts and skim fusion.py + batchpipe.py since they're central to the "fusion" extraction discussion.
$ wc -l src/crxml/*.py src/crxml/stages/*.py && git log --oneline -5 && ls docs/usage/
325 src/crxml/batchpipe.py
  102 src/crxml/fusion.py
   43 src/crxml/__init__.py
   78 src/crxml/parallel.py
   72 src/crxml/pipeline.py
   91 src/crxml/sinks.py
  366 src/crxml/source.py
   49 src/crxml/stages/cast.py
   23 src/crxml/stages/drop.py
   90 src/crxml/stages/filter.py
…
Click to expand
→Read src/crxml/fusion.py 
Thought: 46.4s
Now I have a comprehensive picture of the codebase. Let me summarize what I know:

Repository structure:
- src/crxml_core/ — Rust crate (~3,585 lines):
  - lib.rs (1,196 lines): PyO3 FFI boundary, stream engine (RowParser/CrxmlReader — Crystal-specific XML parsing), 4 columnar entry points, mmap handle, bounded execution, plan-from-kwargs
  - columnar.rs (2,085 lines): BuildPlan, FieldType, FilterPredicate, CompareOp, StrColumn, ColumnBuilder, ColumnarEngine, engines_to_pyarrow_table, plus ~600 lines of inline unit tests with a quick-xml oracle
  - splitter.rs (304 lines): generic-ish XML splitter (find_special_regions, next_row_start, compute_splits) + tests
- src/crxml/ — Python package (~1,273 lines): source.py, pipeline.py, fusion.py, batchpipe.py, parallel.py, sinks.py, stages/
- tests/ — 10 test files (~2,071 lines): differential tests vs ElementTree oracle, fusion property tests, hardening tests, etc.
- fuzz/ — cargo-fuzz targets for parse and splitter

Key observations relative to the user's proposed plan:

1. The Rust core is small — only 3 files, 3.6k lines. The extraction is quite tractable.
2. Crystal-specific parsing is embedded in the columnar engine: parse_bytes_quickxml hardcodes <Field FieldName=...>, <Text Name=...>, <Section SectionNumber=...>, FormattedValue/Value/TextValue. This is the main coupling that must be broken via a decoder API / row-source trait.
3. The stream engine (CrxmlReader) is entirely Crystal-specific — stays in crxml per the user's recommendation (#13).
4. Generic pieces that are genuinely format-independent: BuildPlan, ColumnBuilder/StrColumn, FieldType/casting, FilterPredicate/CompareOp, extend/merge logic, auto_dict upgrade, Arrow export, splitter interface + parallel scheduler, bounded executor skeleton, mmap handle, estimate_batch_params (row-density sampling is format-specific though — it scans for row_tag).
5. PyO3 dependencies are pervasive: columnar.rs imports pyo3 (for Python in to_pyarrow_table, apply_pyarrow). To make a pure Rust core, Arrow export should produce arrow RecordBatches natively and the FFI-to-Python export moves to the python bindings crate. The Compare filter's apply_pyarrow uses pyarrow.compute from Rust — that would move to Python layer or be reimplemented in Arrow compute (arrow-rs has kernel functions: gt, lt, etc. and filter record batch).
6. Value model: currently everything is strings; typed columns parse strings via lexical. The user wants a Value enum so JSON can pass native numbers.

Now — the user's message is essentially a full plan already (probably from another conversation with an LLM). My job: read the repo (done), validate the plan against reality, and present a concrete, actionable implementation plan. I'm in plan mode — read-only, must ask clarifying questions when weighing tradeoffs, then present the final plan.

Points where repo reality differs from / nuances the pasted plan:

1. Scale: The engine core is tiny (3.6k lines). The plan proposes a big multi-crate workspace. That's fine but worth noting we can start leaner: a single new workspace with engine-core, engine-python (pyo3), and later adapters. Actually the user's structure proposal has core/, decoder-api/, python/.

2. Where does the new repo live? Options: (a) new sibling repo/directory, (b) Cargo workspace inside crxml, (c) monorepo restructure. The user said "separate the core engine" and mentions "new project" / "new repo". I should ask: separate git repo vs in-repo workspace first? Given phase 1-4 involves keeping crxml as reference implementation AND making crxml consume the new engine, an interim approach could be a local path dependency or a git submodule. This is a real decision to clarify.

3. Fusion redesign: fusion.py lives in Python and depends on _plan_kwargs. In the new engine, the logical plan (Source → Rename → Cast → Filter → Project → Sink) should exist in Rust core. But crxml's Python pipeline/fusion machinery stays in crxml initially. Phase 4 moves crxml to consume the new engine.

4. Compare filter: currently implemented by calling pyarrow.compute from Rust. In a pure core, this should become an Arrow-native filter using arrow::compute (gt/lt/eq kernels + filter_record_batch), executed on RecordBatch before FFI export. That's actually a nice improvement and keeps pyo3 out of core.

5. Value model: adding Value { Str, Int64, Float64, Bool, Null } to the decoder API. For XML adapter everything is Str — semantics preserved trivially. ColumnBuilder::push_value(Value).

6. Splitter trait: current compute_splits is XML-specific (comment/CDATA regions, <tag boundary detection) but parameterized by row_tag bytes. The generic interface: trait RowSplitter { fn find_splits(&self, bytes, num_chunks) -> Vec<Range<usize>> } plus density estimator trait for bounded mode. CrystalXmlSplitter implements it.

7. Testing/compat invariant: v1.1.0's differential test suite (ElementTree oracle) + fuzz targets become the acceptance tests for the extracted engine. The plan: port columnar.rs inline tests, run existing pytest suite against new-engine-backed crxml.

8. Arrow version pinning: arrow =55.2.0 pinned exactly due to C Data Interface stability — carry over.

9. mimalloc global allocator: lives in the pyo3 binary crate, not core. Fine.

10. What about estimate_batch_params? It uses memmem over row_tag — that's decoder-provided sampling (row density). Belongs behind a trait: RowDensityEstimator.

11. Python package naming: what should the new project be called? Ask user.

12. License/repo ownership: same author presumably MIT.

Also worth asking:
- New standalone repo vs workspace-in-crxml? 
- Name of the new engine crate/package?
- Should crxml immediately switch to consuming the new engine (phase 4) in the same effort, or stop after extraction with the engine validated via compatibility harness? The pasted plan says phases 1-8; phase 4 flips crxml to the new engine. That's ambitious for one go. I'd propose: extract into new workspace, wire crxml to it via path/git dependency, keep old code until parity proven, then delete.
- Language boundary details: keep PyO3 in separate crate (yes).
- Do they want CSV/NDJSON adapters now or just the XML adapter + interfaces? Plan phases say later. Scope question: this session implements which phases?

Let me also quickly peek at batchpipe.py and stages to confirm nothing else is needed... The architecture doc described them thoroughly. I think I have enough.

One more important detail: the columnar.rs.test file — there's a stray file columnar.rs.test in src/crxml_core/src/. Probably a backup. Not critical.

Also check: fuzz/Cargo.toml references crxml-core? Let me not bother; enough context.

Let me now formulate clarifying questions:

1. Repo layout: brand-new standalone repo (sibling directory), or Cargo workspace inside crxml first (e.g., engine/ at top level with crates), moving out later?
2. Project name for the engine?
3. Scope for this effort: Phases 1–4 (extract + crxml consumes it + parity green) or stop earlier?
4. Value model now or later: introduce Value enum in decoder API from day one (recommended since it shapes every signature) vs keep Option<String> initially?

I'll recommend defaults: sibling repo, name suggestion options, scope = phases 1-4 with parity harness, Value enum day one.

Then present the plan. Since plan mode requires me to present a well-researched plan, I should ask the questions first, then finalize the plan based on answers.

Let me ask the questions now.
I've read through the whole repo — the three Rust modules (lib.rs 1196 lines, columnar.rs 2085, splitter.rs 304), the Python layer (source/pipeline/fusion/batchpipe/stages/sinks/parallel), the docs/architecture, tests, and the v1.1.0 state. Your analysis maps cleanly onto what's actually there. A few findings that sharpen (or slightly correct) the plan:
Reality checks against your proposal
- The Rust core is only ~3.6k lines across 3 files, so extraction is very tractable — no need to split into 6 sub-crates on day one; one core crate with modules gets the same boundaries.
- The biggest coupling point you correctly flagged: ColumnarEngine::parse_bytes_quickxml (columnar.rs:816) hardcodes <Field>/<Text>/<Section>/FormattedValue — this is what becomes the RecordDecoder seam.
- One thing your plan didn't mention: FilterPredicate::apply_pyarrow (columnar.rs:189) calls pyarrow.compute from inside Rust. For a pure core this should be reimplemented with arrow::compute kernels (gt/lt/filter_record_batch) so pyo3 disappears from core entirely — a small semantic-parity risk to cover in the differential tests.
- estimate_batch_params (lib.rs:210) samples row density via memmem(row_tag) — that's decoder knowledge and belongs behind a trait, as you said for batch sizing.
- StrColumn, ColumnBuilder, BuildPlan, extend() merge, auto-dict thresholding, and the parallel fast-path/merge-path split are all genuinely format-free already — they lift almost verbatim.
- CrxmlReader/RowParser (lib.rs:739–1114) is 100% Crystal-shaped; stays in crxml, agreeing with your line #13.
Before I write up the final plan, four decisions materially change its shape:
# Questions
Where should the new engine live during and after extraction?
New sibling repo (Recommended)
What should the engine project be called? (affects crate names, PyPI package, module paths)
You propose (Recommended)
How far should this effort go in one pass?
Phases 1–4 (Recommended)
Introduce the generic Value enum (Str/Int64/Float64/Bool/Null) in the decoder API from day one?
Yes, from day one (Recommended)
Thought: 1m 52s
The user answered:
1. New sibling repo
2. I propose the name
3. Phases 1-4 (extract core, decoder API + Crystal adapter, flip crxml to consume new engine, keep v1.1 differential suite green)
4. Value enum from day one

Now let me write the final plan. It should be comprehensive yet concise, detailed enough to execute. Let me think through all details:

Name proposal: Something short, neutral, evocative of "rows → Arrow". Options: "rowframe", "rowset", "strata", "quarry", "coltrane"... Let me pick something clean and likely not taken: 
- rowframe — crate rowframe-core, Python rowframe. Nice.
- arrowfeed — probably conflicts with arrow trademark vibes.
- strata — taken by various.
- rowan — taken (Rust syntax tree library).
- fieldstone? 
- tabular?
- rowframe seems good and descriptive. Crate names: rowframe-core (Rust core), rowframe-py or python package rowframe with extension _rowframe_core. Placeholder module name internally: engine.

I'll propose rowframe with alternatives listed.

New repo layout:

rowframe/
├── Cargo.toml                  # workspace
├── crates/
│   ├── rowframe-core/          # pure Rust, no pyo3
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── value.rs        # Value enum + coercion to typed builders
│   │       ├── plan/
│   │       │   ├── mod.rs      # ExecutionPlan
│   │       │   ├── projection.rs
│   │       │   ├── predicate.rs
│   │       │   └── cast.rs     # FieldType + lexical parsing
│   │       ├── columnar/
│   │       │   ├── mod.rs
│   │       │   ├── str_column.rs
│   │       │   ├── builder.rs  # ColumnBuilder
│   │       │   └── dictionary.rs
│   │       ├── engine.rs       # TableBuilder (was ColumnarEngine minus XML)
│   │       ├── merge.rs        # extend / schema reconciliation
│   │       ├── arrow_export.rs # RecordBatch building + compare-filter via arrow::compute
│   │       ├── decoder.rs      # RecordDecoder trait, RecordSink, RowSplitter trait, RowDensity trait
│   │       ├── parallel.rs     # rayon scheduler over ranges
│   │       ├── bounded.rs      # BoundedExecutor + MemoryBudget
│   │       └── input.rs        # InputBuffer: MmapInput / BytesInput / FileInput
│   ├── rowframe-xml/           # first adapter: Crystal Reports XML decoder + splitter
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── decoder.rs      # CrystalXmlDecoder implements RecordDecoder
│   │       └── splitter.rs     # CrystalXmlSplitter implements RowSplitter (+density)
│   └── rowframe-python/        # PyO3 bindings
│       └── src/
│           ├── lib.rs          # pymodule, exceptions, global mimalloc allocator
│           ├── plan_kwargs.rs  # build ExecutionPlan from Python kwargs
│           ├── ffi.rs          # Arrow C Data Interface export to pyarrow
│           └── reader.rs       # (optional later) generic streaming reader
└── tests/                      # Rust integration + parity harness

Wait — where does the stream engine (CrxmlReader) live? It stays in crxml (it's Crystal-specific). But crxml's CrxmlReader is a PyO3 pyclass in crxml's own crate. crxml keeps its own crate for the stream reader; its columnar functions get replaced by calls into rowframe-python... Hmm, actually there's a subtlety: crxml currently ships ONE extension module _crxml_core containing both stream reader and columnar entry points. After phase 4:

Option A: crxml depends on rowframe as a Rust dependency (path/git), and crxml's _crxml_core crate keeps only CrxmlReader (stream) + thin wrappers that call rowframe-core/rowframe-xml directly (not via Python-level rowframe package). This avoids requiring users to install two packages. 

Option B: crxml python package depends on rowframe PyPI package and re-exports.

Option A is better: no double-install, no version coupling at the PyPI level, and the columnar path goes straight through rowframe-core without an extra FFI hop. The CrystalXMLSource._read_arrow calls into _crxml_core which calls rowframe crates. Actually wait — if rowframe-python exists separately, who uses it? The future multi-format package. For phases 1-4, maybe we don't even need rowframe-python yet! crxml can depend on rowframe-core + rowframe-xml directly in its own cdylib.

Hmm, but the user's plan said the new repo should have engine-python bindings so it becomes "Rust engine that produces Arrow, with Python bindings" usable beyond crxml. For scope phases 1–4, I'd include rowframe-python with the four read_to_* entry points ported generically (they take a format parameter eventually, but for now only crystal xml adapter exists → the python bindings expose read(path, row_tag=...)). Then crxml has two choices...

Let me think about what minimizes risk while matching user intent:
- The new repo must be self-standing with its own Python bindings (that's its reason to exist — bigger than Python/crxml).
- crxml flips to consume the engine. Simplest robust approach: crxml's Rust crate adds path/git deps on rowframe-core + rowframe-xml, deletes its columnar.rs/splitter.rs, reimplements its 4 pyfunctions as thin wrappers over rowframe types, keeps CrxmlReader + RowParser untouched. The pyarrow export code (to_pyarrow_table via ToPyArrow) moves into rowframe-core? No — ToPyArrow needs pyo3. It should be in rowframe-python OR a small shared helper. If crxml calls rowframe-core directly it needs the pyarrow-export helper too. Options:
  - Put pyarrow export behind an optional feature pyarrow-export in rowframe-core using optional pyo3 dep. That leaks pyo3 into core conditionally... user explicitly wants core free of PyObject. Better: put export helpers in rowframe-python and have crxml's wrapper crate ALSO just depend on rowframe-python? That means crxml's cdylib links pyo3 twice (same version, fine — it's the same crate compiled once).
  
Actually simplest: crxml's _crxml_core depends on rowframe-python (which exposes non-macro pub fns like export_batch_to_pyarrow(py, batch)) plus rowframe-core + rowframe-xml. Same pyo3 version across all. That works fine technically.

Alternatively even simpler: crxml Python layer imports rowframe's Python package when present... no — two binary wheels to ship. Keep Option A with rowframe-python as a library dep.

Hmm wait, actually, let me reconsider: is it cleaner for crxml to depend on rowframe-python, or for rowframe-python to BE the provider of everything and crxml just... no, crxml must keep shipping one self-contained wheel (pip install crxml). So crxml's maturin crate vendors the engine via cargo deps. Good: crxml/_crxml_core = CrxmlReader (stream) + thin columnar wrappers calling rowframe crates.

For the parity harness: since both implementations coexist during transition, we can run old vs new in CI before deleting old code. But note the old code gets deleted in phase 4 flip... The compatibility test is: crxml test suite (2071 lines incl. differential vs ElementTree oracle + fuzz targets) passes against new-backed crxml. Plus a direct Rust-side parity harness comparing extracted engine output against golden snapshots generated from v1.1.0 outputs (e.g., frozen JSON of tables for fixture files). Good idea: generate golden parquet/json fixtures from unmodified v1.1.0, check them into rowframe repo tests/golden/.

Key API design decisions to encode in the plan:

1. Value enum: Str(&'a str)/Str ownership question — use Cow-like: Value<'a> { Str(&'a str), Int64(i64), Float64(f64), Bool(bool), Null }. Decoders emit borrowed strings where possible.

2. RecordDecoder trait: push-based (matches current zero-copy perf):
pub trait RecordDecoder {
    fn decode(&mut self, bytes: &[u8], sink: &mut dyn FieldSink) -> Result<DecodeStats, DecodeError>;
}
Hmm, but current ColumnarEngine intermixes decoding & sink. Design: FieldSink receives (field_name: &str, value: Value) events + begin_row()/end_row(). Rename/drop/filter/null-fill happen in engine between decoder and builders. Actually the rename/drop resolution happens per-field today inside push_field_str; in the new design, the engine's TableBuilder implements FieldSink with plan-aware dispatch. Filter check needs access to builder values (get_filter_value) — stays in engine at end_row, same as finish_row today. Last-write-wins duplicate-field pop/push logic stays in TableBuilder. Decoder contract details to preserve:
  - last-write-wins within a row
  - null-fill missing fields at end_row
  - partial trailing row discarded (normalize)
  - unknown children → empty-string field present
  - filter Equal/NotEqual semantics on string form including missing→kept-for-!=

3. Splitter trait + density estimator:
pub trait RowSplitter {
    fn find_special_regions(&self, bytes:&[u8]) -> Vec<Range<usize>> { vec![] }
    fn next_row_start(&self, bytes:&[u8], from:usize) -> Option<usize>;
    fn compute_splits(&self, bytes:&[u8], n:usize) -> Vec<Range<usize>>;
    fn estimate_bytes_per_row(&self, sample:&[u8]) -> usize;
}

4. Parallel executor: generic over splitter + decoder factory: takes chunks, spawns engines via rayon, fast-path export vs auto_dict merge — ported nearly verbatim from lib.rs:97-180 but calling trait objects.

5. Bounded executor: port parse_columnar_bounded loop; sampling density comes from splitter trait; concat_tables via pyarrow stays in python crate (needs pa). Hmm — bounded mode concatenates pyarrow tables in Rust today. In pure core, bounded executor should return Vec<RecordBatch-chunks> or an iterator of batches; the python layer concatenates via pyarrow. Cleaner: core returns Vec<Vec<RecordBatch>> or yields batches through callback; python binds them into one table. Also concat_tables promote_options="default" handles auto_dict schema promotion — python-side concern.

6. Compare filter: implement with arrow::compute::{gt, lt, ge, le, eq, ne} + filter_record_batch in core (replacing apply_pyarrow). Parity risk: pyarrow.compute vs arrow-rs kernel null semantics — need differential tests (both treat null comparison as null→filtered out by default... actually pc.greater returns null mask; table.filter drops nulls. arrow-rs gt produces BooleanArray with nulls where either side null; filter_record_batch with nulls in filter treats them as false? Need care: arrow-rs filter treats null as false by default. pyarrow filter default null_selection_behavior="drop" — also drops. Equivalent. Still, add explicit parity tests.)

7. ExecutionPlan naming: user asked BuildPlan → ExecutionPlan/IngestPlan. Fields: renames (HashMap<String,String>), drop (HashSet), casts (HashMap<String,FieldType>), dictionaries (HashSet), predicate, schema_order, auto_dict. Keep resolve order rename-then-drop identical.

8. Exceptions: PlanError/MergeError/XmlError stay in binding layers (rowframe-python defines its own; crxml maps them to its existing exception classes for backward compat).

9. Feature flags: core has no features except maybe rayon always-on; mmap input behind mmap feature carrying unsafe; arrow pinned =55.2.0 with exact-pin comment preserved. mimalloc stays in each cdylib crate.

10. What gets deleted from crxml in phase 4: columnar.rs (engine+plan+builders), splitter.rs, the columnar pyfunctions/mmap/bounded code in lib.rs — replaced by ~200-line wrapper. CrxmlReader/RowParser stays. fuzz targets updated to hit new engine through wrapper. Tests: keep all pytest files; adjust any that poke internals (test_parse* helpers re-exposed by wrapper).

11. Golden parity harness: script scripts/export_golden.py runs v1.1.0 over fixtures × {engines × plans}, dumps tables as JSON (or parquet); rowframe repo has matching runner asserting equality. Since after phase 4 crxml IS the new engine, goldens guard against drift during extraction.

12. Versioning/repo setup: MIT license, Cargo workspace, rust-toolchain pin, CI running fmt/clippy/test/fuzz-smoke + parity job checking out crxml@v1.1.0 tag.

13. Name: propose "rowframe": crates rowframe-core, rowframe-xml, rowframe-python; PyPI rowframe; ext module _rowframe. Alternatives: rowset, tabline, colhead. I'll go with rowframe unless they object.

Also worth noting in the plan: what NOT to extract (CrxmlReader, Crystal grammar, fusion.py/batchpipe.py/stages — Python pipeline machinery stays in crxml; rowframe-python gets a minimal source object later, not now).

Milestones with concrete steps:

Phase 0: prep
- Tag crxml v1.1.0 (already tagged presumably), create sibling repo, scaffold workspace, set up CI skeleton.

Phase 1: extract core (rowframe-core) — mechanical lift:
1. value.rs: Value enum + From<&str> etc.
2. columnar/: str_column.rs (StrColumn verbatim, pub), builder.rs (ColumnBuilder push_value(Value)/push_str), dictionary.rs (dict_code + upgrade heuristic).
3. plan/: FieldType, ExecutionPlan (renamed BuildPlan, same fields/semantics), CompareOp, Predicate (Equal/NotEqual string-form + Compare), resolve_field/column_type.
4. engine.rs: TableBuilder = ColumnarEngine minus parse_bytes*: ensure_column/schema_insert_index/push_field(value: Value)/finish_row/extend/auto_dict_upgrade/normalize/reset/sort_columns.
5. merge.rs: extend_owned + engines_to_batches unified-order logic (without pyo3): produce Vec<RecordBatch>.
6. arrow_export.rs: builder→ArrayRef, schema, RecordBatch; compare-predicate via arrow::compute kernels.
7. decoder.rs traits + parallel.rs + bounded.rs + input.rs (MmapHandle moved, advice logic).
8. Unit tests: port columnar.rs inline tests (oracle etc.) against TableBuilder + a tiny test decoder; port splitter interface tests.

Phase 2: adapters (rowframe-xml):
1. CrystalXmlDecoder implementing RecordDecoder — port the three parse loops (fast quickxml path + parse_tail fallback) but emitting FieldSink events instead of touching builders. Careful: current code pushes directly; the tail-fallback parser pushes owned Strings — fine, Value::Str(&str) from owned temp... need small lifetime handling: sink takes &str; decoder owns scratch.
2. CrystalXmlSplitter implementing RowSplitter (find_special_regions/next_row_start/compute_splits/estimate_bytes_per_row — memmem density).
3. Port splitter unit tests + fuzz target seeds.

Phase 3: parity proof (before flipping crxml):
1. Golden fixtures: run stock v1.1.0 (pip install from tag) over a matrix of XML inputs (tests fixtures, bench_data samples if licensable, fuzz seeds) × plans (rename/drop/types/dict/filter eq-ne/compare/schema_order/auto_dict) × engines (columnar/multi/par/bounded) → dump JSON.
2. rowframe harness decodes same inputs via CrystalXmlDecoder + TableBuilder and compares semantically (values + types + order + nulls).
3. Fuzz: port fuzz_targets/parse.rs to drive decoder+sink; splitter fuzz on trait.

Phase 4: flip crxml:
1. crxml Cargo.toml adds path/git deps rowframe-core, rowframe-xml, rowframe-python(export helpers)... wait — do we need rowframe-python for export? crxml needs ToPyArrow export of RecordBatch. rowframe-python will have pub fn record_batches_to_pyarrow_table(py, &[RecordBatch]) -> PyResult<PyObject> style helpers (non-#pyfunction so reusable). Yes, crxml depends on it. Alternative: replicate 30-line export in crxml. Prefer reuse.
2. Rewrite crxml's read_to_columnar* wrappers (~250 lines): build ExecutionPlan from kwargs (keep PlanError messages byte-compatible!), call rowframe, map errors to XmlError/MergeError.
3. Keep exception names/kwargs/messages identical (tests assert messages? check test_hardening.py — likely yes; keep format strings).
4. Delete old columnar/splitter modules; keep CrxmlReader untouched.
5. Run full pytest suite + benchmarks sanity (compare timings, accept ±10%).
6. Update docs/architecture.md to describe new layering.

Risks section:
- pyarrow.compute vs arrow-rs compare/filter null semantics → explicit parity tests.
- Error message drift breaks hardening tests → copy message formats exactly.
- Performance regressions from dyn-trait indirection in hot loop → keep monomorphic generics (impl Trait/static dispatch) in hot paths; enum DispatchDecoder alternative if measurable.
- Arrow C Data Interface pin (=55.2.0) — carry pin + comment.
- Bounded-mode concat semantics (promote_options) move to python wrappers; verify auto_dict bounded path.
- mimalloc global allocator defined once per cdylib — ensure no duplicate-symbol issues when crxml links rowframe-python (it won't; allocator only declared in final cdylib crates).

Deliverables list + acceptance criteria:
- rowframe repo: workspace builds clippy-clean, 233-test equivalent coverage story, parity harness green, fuzz smoke green.
- crxml: full pytest suite green on new backend, README/docs updated, version minor bump (1.2.0).

Open items I should mention but not block: CSV/NDJSON phases come later; streaming reader generic (RowFrameReader) deferred.

Estimated size: core ~1.5k lines lifted + ~600 new; xml adapter 700; python crate 400; crxml diff mostly deletions (-2.4k) + wrapper.

I think the plan is ready. Present concisely with structure. Keep it tight but complete.
All decisions locked. Here's the final plan.
Plan: Extract the generic engine from crxml → new rowframe repo
Naming proposal
- Project: rowframe ("rows → frames of Arrow")
- Crates: rowframe-core, rowframe-xml, rowframe-python; PyPI package rowframe, extension _rowframe
- Easy to globally rename if you pick something else later.
Target layout (new sibling repo)
rowframe/
├─ Cargo.toml                     # workspace
├─ crates/
│  ├─ rowframe-core/              # PURE Rust: no pyo3, no quick-xml
│  │  └─ src/
│  │     ├─ value.rs              # Value{Str(&str),Int64,Float64,Bool,Null}
│  │     ├─ plan/                 # ExecutionPlan (ex-BuildPlan), Predicate,
│  │     │                        #   CompareOp, FieldType, cast rules
│  │     ├─ columnar/             # StrColumn, ColumnBuilder, dictionary logic
│  │     ├─ engine.rs             # TableBuilder (ex-ColumnarEngine, XML removed)
│  │     ├─ merge.rs              # extend(), unified-order batch assembly
│  │     ├─ arrow_export.rs       # RecordBatch building; Compare filter via
│  │     │                        #   arrow::compute (replaces apply_pyarrow)
│  │     ├─ decoder.rs            # traits: FieldSink, RecordDecoder, RowSplitter
│  │     ├─ parallel.rs           # rayon scheduler (fast path / auto_dict merge)
│  │     ├─ bounded.rs            # MemoryBudget + BoundedExecutor loop
│  │     └─ input.rs              # InputBuffer: MmapInput (unsafe isolated) /
│  │                              #   BytesInput; prefault/sequential advice
│  ├─ rowframe-xml/               # FIRST ADAPTER ONLY
│  │  └─ src/                     # CrystalXmlDecoder (RecordDecoder impl),
│  │                              #   CrystalXmlSplitter (RowSplitter impl),
│  │                              #   density estimator
│  └─ rowframe-python/            # PyO3: pymodule, kwargs→ExecutionPlan,
│     └─ src/                     #   Arrow C Data Interface export helpers
│                                 #   (pub fns reusable by crxml), mimalloc
└─ tests/                         # ported unit tests + golden parity harness
Key API shapes (decided up front):
// decoder.rs — push-based, zero-copy friendly
trait FieldSink { fn begin_row(&mut self); fn field(&mut self, name:&str, v:Value);
                  fn end_row(&mut self); }          // end_row commits/filters row
trait RecordDecoder { fn decode(&mut self, bytes:&[u8], sink:&mut dyn FieldSink)
                      -> Result<(),DecodeError>; }
trait RowSplitter { fn compute_splits(&self,&[u8],usize)->Vec<Range<usize>>;
                    fn estimate_bytes_per_row(&self,&[u8])->usize; }
Value from day one: XML adapter emits Value::Str everywhere → bit-identical behavior, and JSON/CSV later get native numbers for free.
Phase 1 — Extract core (mechanical lift)
Lift near-verbatim from columnar.rs: StrColumn (columnar.rs:212), ColumnBuilder (287), dict-code/auto-dict heuristics, BuildPlan fields + resolve_field rename-then-drop ordering (99) → renamed ExecutionPlan with same semantics; ColumnarEngine minus parse_bytes* → TableBuilder (ensure_column/schema_insert_index/last-write-wins/finish_row/normalize/extend/sort_columns); extend_owned merge; parallel fast-path vs auto_dict merge split (lib.rs:97–180) genericized over RowSplitter + decoder factory; bounded loop (lib.rs:236–334) with density from the splitter trait; MmapHandle + advice logic (lib.rs:351–394).
Deliberate change: FilterPredicate::Compare executes via arrow::compute kernels + filter_record_batch instead of calling pyarrow.compute from Rust (columnar.rs:189). Carry the exact arrow = "=55.2.0" pin and its stability comment. Hot paths stay statically dispatched (generics, not dyn).
Phase 2 — Crystal XML adapter
Port the quickxml event loops (columnar.rs:816–1085 fast path + parse_tail fallback 1090) and splitter.rs as CrystalXmlDecoder/CrystalXmlSplitter emitting Value events into FieldSink. Preserve exactly: row attributes, Field/Text/Section/unknown-child rules, FormattedValue/Value/TextValue precedence, entity-probe unescape optimization, EOF-mid-row partial-row discard.
Phase 3 — Parity proof (the v1.1 invariant)
1. Generate goldens from unmodified crxml v1.1.0 (installed from the tag) over fixture × plan (rename/drop/casts/dict/eq/ne/compare/schema_order/auto_dict) × engine (columnar/multi/par/bounded) matrices → JSON dumps checked into rowframe.
2. Harness asserts semantic equality (values, types, column order, nulls) from the new stack.
3. Port fuzz targets (fuzz_targets/parse.rs, splitter.rs) onto the trait APIs.
4. Explicit differential tests for the one real semantic risk: pyarrow.compute vs arrow-rs compare/filter null handling.
Phase 4 — Flip crxml onto the engine
- src/crxml_core/Cargo.toml gains path deps on the three rowframe crates; delete columnar.rs, splitter.rs, and the columnar half of lib.rs (~2.5k lines removed).
- Reimplement read_to_columnar{,_multi,_par,_bounded} as thin wrappers (~250 lines): kwargs → ExecutionPlan (byte-identical error messages — test_hardening.py asserts them), call rowframe, map errors to existing XmlError/PlanError/MergeError classes. Reuse rowframe-python's pub export helpers for the pyarrow boundary.
- CrxmlReader/RowParser untouched. Fusion/batchpipe/stages/sinks untouched.
- Run the full 2071-line pytest suite + benchmark smoke (accept ≤10% delta); bump to 1.2.0; update docs/architecture.md.
Not in scope (explicitly)
CSV/NDJSON/generic-XML adapters, generic streaming reader, moving fusion to a Rust logical plan — interfaces are designed for them, nothing built.
Acceptance: rowframe CI green (clippy/tests/fuzz-smoke/parity); crxml suite fully green on the new backend with unchanged public API.
Want me to start executing Phase 1 (scaffold the rowframe repo and begin the core lift)?
▣  Plan · Ox Alpha Free (Unlimited) · 6m 50s
Take this into account

Yes — the core architecture generalizes surprisingly well, but the split has to be deliberate. The key is separating **“how do I find row boundaries and extract fields from this format?”** from **“how do I turn rows into typed columns, parallelize, bound memory, and push down operations?”**

The second part is largely format-agnostic. The first is inherently format-specific.

---

## What’s reusable from crxml as-is

These parts are already format-independent in spirit:

- **Columnar engine** — `StrColumn`, typed `ColumnBuilder`, dictionary encoding, Arrow export via C Data Interface.
- **Parallel orchestration** — rayon chunks, splitter/merge logic, bounded-memory re-read driver.
- **Memory management** — mmap with synchronous unmap, arena allocators, `MADV_*` hints.
- **Pushdown/fusion planner** — the idea of compiling `RenameFields`, `DropFields`, `CastTypes`, `FilterRows` into a `BuildPlan` that runs inside the parse loop.
- **Python API layer** — `Pipeline`, stages, sinks, engine dispatch.

All of these can be lifted into a generic core crate without knowing whether the input is XML, JSON, CSV, or HTML.

---

## What must stay format-specific

These are the parts that cannot be shared blindly:

- **Row-boundary splitting**  
  - XML/HTML: find `</row_tag>` or `<row_tag` boundaries, skip comments/CDATA, handle tag-prefix collisions.
  - JSON: brace/bracket balancing, skip strings/comments, handle nested objects.
  - CSV: newline outside quotes, escaped quotes.
  - JSONL: newline splitting (easy).

- **Row extraction grammar**  
  - XML: attributes + child `Field`/`Text`/`Section` rules.
  - JSON: object keys → values; nested flattening rules.
  - CSV: header row → columns; no nesting.
  - HTML: repeated elements → row; attributes/text children; often needs tolerant parsing.

- **Schema inference**  
  - XML/CSV values start as strings; you may infer types later.
  - JSON has native types (`string`, `number`, `bool`, `null`).
  - HTML is strings by default, but `<td data-type="int">` could hint types.

- **Pushdown mapping**  
  - `filter={"field": "Status", "op": "==", "value": "Active"}` must translate to format-specific checks:
    - XML: compare `<Field FieldName="Status"><Value>…`
    - JSON: compare object key `Status`.
    - CSV: compare column index.
    - HTML: compare extracted text.

---

## How I’d separate it

I would define three or four Rust traits, then implement them per format.

### 1. `RowSource` / `Splitter`

Responsible for cutting a byte buffer into chunks that each start at a valid row boundary.

```rust
trait Splitter: Send + Sync {
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize>;
    fn estimate_rows_per_byte(&self, sample: &[u8]) -> f64;
    fn validate(&self, bytes: &[u8]) -> Result<()>;
}
```

Implementations:
- `CsvSplitter` — memchr for newline, quote-state tracking.
- `JsonSplitter` — brace counting, string/comment skipping.
- `XmlSplitter` — essentially crxml’s current `splitter.rs`.
- `JsonLinesSplitter` — trivial newline split.

### 2. `RowParser`

Turns a byte chunk into a sequence of “field events” or directly fills column builders.

```rust
trait RowParser: Send + Sync {
    fn parse_chunk(
        &self,
        bytes: &[u8],
        plan: &BuildPlan,             // generic pushdown plan
        sink: &mut dyn ColumnarSink,  // generic column writer
    ) -> Result<()>;
}
```

Implementations:
- `XmlRowParser` — quick-xml event loop + CR flattening grammar.
- `JsonRowParser` — serde-json streaming or custom object scanner.
- `CsvRowParser` — csv crate, header mapping.
- `HtmlRowParser` — html5ever or equivalent, repeated element extraction.

### 3. `BuildPlan` / Pushdown

This can be **fully format-agnostic**:

```rust
struct BuildPlan {
    field_map: HashMap<String, String>,
    drop_fields: HashSet<String>,
    field_types: HashMap<String, FieldType>,
    dictionary_columns: HashSet<String>,
    filter: Option<FilterPredicate>,
    schema_order: Vec<String>,
    auto_dict: bool,
}
```

The format-specific parser checks the plan when extracting each field. For example, CSV parser maps `field_map` keys to header indices; JSON parser maps object keys; XML parser maps attribute/child names.

### 4. `ColumnarSink`

Already exists in crxml, just make it trait-based:

```rust
trait ColumnarSink {
    fn begin_row(&mut self);
    fn put_field(&mut self, name: &str, value: Value);
    fn end_row(&mut self);
    fn finish(self) -> RecordBatch;
}
```

`Value` would be an enum covering `String`, `Int64`, `Float64`, `Bool`, `Null`. This is where casting and dictionary encoding happen, completely independent of input format.

---

## Format-by-format feasibility

| Format | Difficulty | Notes |
|--------|------------|-------|
| **CSV / TSV** | Low | Splitter is easy; parser is simple; all values start as strings. Reuse columnar engine unchanged. |
| **JSON** | Medium | Need brace-balanced splitter; parser can use serde-json but be careful about streaming and memory. Native types simplify pushdown. |
| **JSONL** | Low | Newline split; each line is a JSON object; perfect for parallelism. |
| **XML** | Medium | Already done in crxml. Generalize row grammar to arbitrary tags. |
| **HTML** | Harder | Not always well-formed; need tolerant parser (`html5ever`, `scraper`). Row extraction requires a selector or repeated tag. |
| **Parquet / Avro / Arrow IPC** | Separate path | Binary formats; splitter and parser are very different, but columnar sink and pushdown still apply. |

---

## What I’d build

The result would look a lot like **Apache DataFusion’s `FileFormat` / `ListingTable`** design, but with crxml’s focus on **zero-copy, GIL-free, pushdown, and memory-bounded execution**.

A new crate, say `rowreader-core`, would contain:

- `Splitter` trait
- `RowParser` trait
- `ColumnarSink` / `ColumnBuilder`
- `BuildPlan` and pushdown logic
- Parallel and bounded drivers
- Arrow export

Then format crates:

- `rowreader-xml`
- `rowreader-json`
- `rowreader-csv`
- `rowreader-html`

The Python layer (`CrystalXMLSource` → `RowSource`) would become a generic `FileSource(format=JsonFormat(), ...)` or keep specific classes per format.

---

## Bottom line

Yes, you can abstract crxml’s core + engine to handle JSON, XML, HTML, CSV, and more. The separation line is:

**Format-specific**: splitting, row extraction, schema inference  
**Format-agnostic**: columnar storage, parallelism, memory bounding, pushdown planning, Arrow export, Python API

The hardest part is not the columnar engine — it’s the splitter and parser for each new format, because that’s where the messy real-world format quirks live. But the architecture you already have is a solid foundation for exactly that generalization. 
Thought: 1m 13s
The user has pasted another analysis (likely from another AI conversation) that largely agrees with my plan but introduces some different naming and structural ideas:

1. Splitter trait with find_split_points, estimate_rows_per_byte, validate
2. RowParser trait — notably, this conflicts with my earlier decision (and the user's own pasted plan point #13: "Do NOT extract RowParser") — wait, actually in the first message, the user's plan said "Do NOT extract RowParser" referring to crxml's specific RowParser struct (the Crystal-specific stream parser in lib.rs). This new message uses "RowParser" as the name for the generic per-format field-extraction trait — which is what I called RecordDecoder. So it's a naming difference, not a semantic conflict. The new message's XmlRowParser = my CrystalXmlDecoder.
3. ColumnarSink trait — matches my FieldSink (begin_row/put_field/end_row) plus a finish() -> RecordBatch.
4. Value enum — agrees with what we already decided.
5. BuildPlan stays format-agnostic — agrees.
6. New crate name suggestion: rowreader-core with rowreader-xml/json/csv/html. We already agreed on rowframe. The user said earlier "You propose" for naming and I proposed rowframe. This new message suggests rowreader. I should ask or just note that this is the same architecture with a different name; keep rowframe unless they prefer rowreader.
7. It also mentions schema inference as format-specific concern.
8. It mentions the Python layer becoming generic FileSource(format=...) or per-format classes.

So my job now: integrate this feedback into the final plan. Points to reconcile:

- Trait naming: Their message names the two key traits Splitter and RowParser + ColumnarSink. My plan had RecordDecoder, FieldSink, RowSplitter. There's a genuine collision risk: crxml already HAS a type named RowParser (lib.rs:739) — reusing that name for the generic trait would be confusing in the codebase history/docs. I should flag this and propose naming: keep trait names distinct from crxml's existing types. Options:
  - Splitter / Decoder (or RecordParser) / ColumnarSink
  - I'll propose: Splitter, RecordParser (avoiding literal RowParser collision), ColumnarSink — adopting their vocabulary where safe.

Actually, let me think about whether to just adopt their names wholesale since the user is endorsing this direction ("Take this into account"). The cleanest synthesis:
  - Splitter (their name; mine was RowSplitter — theirs is shorter)
  - RowParser — but crxml has struct RowParser in lib.rs. After phase 4, crxml keeps that struct. If the new repo defines trait RowParser, there are two things named RowParser across the repos. Within each repo it's unambiguous, but crxml will depend on rowframe crates → potential import ambiguity in crxml's crate. Can alias on import (use rowframe_core::RowParser as _). Rust traits can be imported anonymously (as _) so methods resolve without naming. It's workable but I'd still slightly prefer RecordParser. I'll present the recommendation but note the tradeoff, maybe make it a question option... The instructions say ask when weighing tradeoffs. I've already asked 4 questions last turn; the user gave a directive "Take this into account". I think one focused question set is appropriate for: (a) trait naming choice, (b) crate name rowframe vs rowreader, (c) whether to add the validate() method & estimate_rows_per_byte f64 vs my usize bytes_per_row, (d) scope of ColumnarSink::finish returning RecordBatch vs engine-owned export.

Hmm, but over-asking is bad. Let me instead fold these into thtyped Values (JSON native), which IS the schema-inference channel. CSV typed inference later via optional infer step. Fine.

- Format-by-format feasibility table: matches phased roadmap (CSV phase 5, NDJSON 6, generic XML 7, HTML/JSON 8). No change needed beyond confirming interfaces support them (e.g., HtmlRowParser needs selector config — future).

- Python layer: generic FileSource(format=...) — out of scope now; crxml keeps CrystalXMLSource; rowframe-python exposes minimal generic read API. Already in plan.

- Binary formats note (Parquet/Avro): separate path, out of scope. Mention as non-goal.

Also the system reminder says plan mode active again — read-only. So I must NOT execute. I should produce the revised plan incorporating this feedback, ask remaining clarifying questions (naming), then finalize.

Let me do a quick sanity check on anything else in the message that changes structure materially:

- They propose parse_chunk(&self, bytes, plan, sink) signature passing plan into parser. In my design, TableBuilder holds the plan and implements FieldSink; parser doesn't see the plan. Which is better?
  - Passing plan to parser enables format-specific pushdown optimization inside the parser (e.g., skip extracting dropped fields early, apply rename at source). But crxml currently applies rename/drop in push_field_str (engine side), and the fast-path skips plan lookup when empty. Keeping plan application centralized in the sink/engine means one implementation, easier parity; parsers stay dumb. However there's a real perf argument for letting parsers skip work (e.g., don't even extract <Text> values for dropped fields).
  - Middle ground: give the sink an is_field_wanted(name) -> bool style hint or pass a lightweight PlanView to parsers. Actually simplest faithful-to-crxml approach: engine-side filtering exactly as today (resolve_field in ensure/push path), plus optional parser.hint(plan) for skipping extraction. For phases 1–4 parity, engine-side only. I'll spec parse_chunk(&self, bytes, sink) and keep plan in engine; note the future hint optimization. Hmm, but their design explicitly passes plan to parser. To "take into account" without breaking parity-first principle, I'll define the trait with an optional plan_hint() accessor: trait RecordParser { fn parse_chunk(&self, &[u8], &mut dyn ColumnarSink) -> Result<()>; } and drivers construct parser with config (e.g., XmlParserConfig{row_tag}) — config ≠ pushdown. And add ColumnarSink::wants(name) cheap check used by engines; parser may call sink.wants(name) to skip extraction entirely — that gives parsers the ability to avoid wasted extraction without coupling them to BuildPlan internals. Nice synthesis. I'll include that.

- finish(self) -> RecordBatch on ColumnarSink: current export goes through TableBuilder.to_arrow_array per column + schema + optional compare-filter post-reduce + auto_dict upgrade ordering. finish() returning RecordBatch is fine if auto_dict_upgrade + compare-filter run in driver before/after finish. Spec: finish(&mut self) -> Vec<RecordBatch> hmm, engines_to_pyarrow_table builds batches from multiple engines. Keep sink.finish() -> RecordBatch (single chunk); driver merges batches. auto_dict upgrade must run pre-finish (mutates builders). Compare filter runs post-finish on the batch/table — in core via arrow kernels. OK.

Now the questions to ask (keep short, 2 max):
1. Project/crate name: rowframe (my earlier proposal, you accepted "you propose") vs rowreader (this message's suggestion)?
2. Trait vocabulary: adopt Splitter/ColumnarSink verbatim, and for the parser trait: RowParser (matches this doc; collides with crxml's existing struct name) vs RecordParser (avoids collision)?

Maybe also confirm: nothing else changes scope (still phases 1–4)? The message doesn't change scope. Don't re-ask.

Then present updated plan deltas concisely — full plan restated briefly with the integrated changes. The previous turn already presented the full plan; this turn should show what changed + consolidated final plan. Keep it tight.

Let me write the response: brief acknowledgment of what this changes/adds (mostly naming + three concrete interface refinements), then the question(s), then after answers, finalize. Since plan mode requires asking before finalizing when weighing tradeoffs, and naming is genuinely open, I'll ask first, then present the consolidated plan in the same turn after tool result comes back.
This refines the plan in three useful ways — mostly interface vocabulary and two structural details — while confirming the same format-specific/format-agnostic split. Before I lock the revision, two naming decisions are now genuinely ambiguous because your latest message proposes different names than what we settled earlier:
1. Project name: earlier you had me propose (rowframe); this writeup suggests rowreader-core.
2. Parser trait name: the writeup calls it RowParser — but crxml already has struct RowParser (lib.rs:739, the Crystal stream parser that stays in crxml). Reusing the exact name creates cross-repo shadowing once crxml depends on the new crates.
# Questions
Which project name should the final plan use?
rypipe? Rust + Python pipe
How should the decoder-side traits be named?
Splitter/RecordParser/Sink (Recommended)
Thought: 20.1s
The user chose a custom name: rypipe ("Rust + Python pipe"). And Splitter/RecordParser/Sink trait naming.

Now I present the final consolidated plan incorporating:
1. Name: rypipe → crates rypipe-core, rypipe-xml, rypipe-python; PyPI rypipe; extension _rypipe.
2. Traits: Splitter, RecordParser, ColumnarSink (with FieldSink events merged into ColumnarSink per the writeup: begin_row/put_field/end_row/finish).
3. From the latest writeup:
   - Splitter gets validate()-style concern → assigned to parser (format knows encoding rules) — actually their Splitter has validate; let me just adopt: Splitter::find_split_points + estimate; RecordParser::validate (default UTF-8 SIMD). Hmm, their design puts validate on Splitter. Either is fine; I'll put a default-method validate on both? No — keep it simple: put validate(&self, bytes) -> Result<(),DecodeError> on RecordParser since encoding validity is format knowledge (CSV encodings etc.), and splitters only need byte scanning. But honestly adopting their signature verbatim reduces friction... Their Splitter::validate likely means "is this input splittable/valid". I'll include validate on RecordParser and note why (encoding/format-level validation; splitter stays pure byte-scanning). Fine to deviate with rationale.
   - Pushdown mapping format-specificity: solved by normalizing field identity at the sink boundary; parsers resolve native identifiers (XML child names, JSON keys, CSV header indices) to names before put_field. BuildPlan remains fully generic. Parsers get skip-hints via ColumnarSink::wants(name) so they can avoid extracting dropped fields without seeing the plan.
   - Schema inference: out of scope for 1–4; Value enum is the inference channel later (JSON native types).
   - finish() -> RecordBatch on sink; driver composes.
4. Everything else from my previous plan stands (phases 1–4, goldens, parity, crxml flip, arrow pin, compare-filter via arrow-rs kernels, bounded executor, InputBuffer, mimalloc in cdylib crates).

Structure of response: concise final plan. Since this is the second revision, I should present the complete updated plan but keep it tight — highlight deltas first, then the consolidated spec. Plan mode: read-only, no execution. End by asking to begin implementation? The user will switch modes when ready.

Let me draft.

Key deltas section:
- Name: rypipe everywhere.
- Trait trio renamed/specified: Splitter / RecordParser / ColumnarSink.
- ColumnarSink absorbs FieldSink (begin_row/put_field(Value)/end_row + wants(name) hint + finish()).
- RecordParser owns validate (UTF-8/encoding) — rationale.
- Pushdown stays generic via name normalization at sink boundary + wants() skip hints (answers their "pushdown mapping is format-specific" point without leaking plan into parsers).
- Schema inference deferred; Value is the channel.

Then final architecture sketch + phases (compact), acceptance criteria.

Keep it reasonably short — this is a revision, not a fresh doc. But it should be self-contained enough to execute from. Medium length.
Locked: rypipe (rypipe-core, rypipe-xml, rypipe-python; PyPI rypipe, extension _rypipe), traits Splitter / RecordParser / ColumnarSink. Here is the final consolidated plan, integrating your latest writeup's interface refinements into the previously agreed scope (phases 1–4, sibling repo, Value day one).
Final plan: extract rypipe core from crxml
Interface layer (the part this revision changes)
// Format-specific side ─ implemented once per input format

trait Splitter: Send + Sync {
    /// Byte offsets where whole rows begin; chunk [0, s1)[s1, s2)...[sn, len)
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize>;
    /// For bounded mode; current crxml equivalent = memmem row-tag density over 64KB
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;
}

trait RecordParser: Send + Sync {
    /// Encoding/format sanity before work (default: simdutf8 pass).
    /// Lives here, not on Splitter: encoding rules are format knowledge;
    /// splitting stays pure byte-scanning.
    fn validate(&self, bytes: &[u8]) -> Result<(), DecodeError> { ... }
    /// One chunk → field events. Never sees BuildPlan.
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink)
        -> Result<(), DecodeError>;
}

// Format-agnostic side ─ lives in rypipe-core, implemented once

enum Value<'a> { Str(&'a str), Int64(i64), Float64(f64), Bool(bool), Null }

trait ColumnarSink {
    fn begin_row(&mut self);
    fn put_field(&mut self, name: &str, value: Value);
    fn end_row(&mut self);                       // null-fill + filter commit/pop
    /// Parser-side skip hint: lets parsers avoid extracting dropped fields
    /// WITHOUT coupling them to BuildPlan internals
    fn wants(&self, name: &str) -> bool { true }
    fn finish(&mut self) -> Result<RecordBatch>; // after auto_dict_upgrade
}
Two points from your writeup resolved by construction:
- "Pushdown mapping is format-specific" — only field identity is format-specific (XML child names vs JSON keys vs CSV header indices). Each parser normalizes its native identifiers to plain names before put_field (CSV maps header→index internally), so ExecutionPlan (renames, drops, casts, dictionaries, predicate, schema_order, auto_dict — same semantics as BuildPlan, incl. rename-before-drop ordering) stays fully format-agnostic in core.
- Schema infee updated plan with clear recommendations, and ask only about genuinely open items:

1. Crate/project name: they suggested rowreader-core; earlier they let me propose and I said rowframe. Now this message says "say rowreader-core". Is the user switching preference? Ambiguous. Worth one quick question.
f scope for phases 1–4; note extension hook: parsers may emit 
2. Trait names: adopt Splitter/RowParser/ColumnarSink vocabulary. I'll recommend Splitter + RecordParser(alias note re: RowParser collision) + ColumnarSink, but honestly if the user wants RowParser literally, use ... as _ handles it. Could fold into the same question.

Other technical deltas from the new message that need incorporation:

- validate(&self, bytes) method on Splitter — quick validation of input before splitting (e.g., XML well-formedness probe, UTF-8 check?). Current crxml does simdutf8 validation in parse_bytes entry, not splitter. I can incorporate validate into the trait with default no-op... Actually where does UTF-8 validation live in new design? Currently TableBuilder.parse_bytes does simdutf8 then parse. In the new split, decoder owns parsing; validation should happen once per chunk before decode — put it in the driver: fn validate(bytes) -> Result<()> on RecordParser or Splitter. I'll assign it to the parser (it knows the format's encoding rules; CSV might be latin-1 etc.). Reasonable: put validate on the parser trait with default = UTF-8 SIMD check.

- estimate_rows_per_byte -> f64: current code computes bytes_per_row usize from 64KB sample. Either representation works; f64 rows/byte vs usize bytes/row — trivial. Keep estimate_bytes_per_row(&sample) -> usize internally but expose whichever; not worth a question. I'll mention it.

- Pushdown mapping being partly format-specific: their point — filter translation differs per format because field identity differs (XML child-name vs JSON key vs CSV column index). In my design, decoders normalize field identity to &str names at the sink boundary, so pushdown stays fully generic in the core; each parser resolves its native field identifiers to normalized names before calling put_field. For CSV, header mapping happens inside CsvParser (header row → index map), so by the time fields hit the sink they're named. That preserves "BuildPlan fully format-agnostic" — agree with them, and my design already does this. Good—state it explicitly.

- Schema inference as format-specific: current crxml does no inference (strings until cast). Keep inference out o
