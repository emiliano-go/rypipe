use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, DictionaryArray, Float64Array, Int32Array, Int64Array,
    PrimitiveArray, StringArray,
};
use arrow::datatypes::{
    DataType, Int32Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, TimeUnit,
};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use crate::plan::FieldType;
use crate::value::Value;
use crate::Result;

/// A borrowed, typed view of a single stored value, used for native-typed
/// filter evaluation without allocating.
#[derive(Debug, PartialEq)]
pub(crate) enum TypedValue<'a> {
    Str(&'a str),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    /// Days since the Unix epoch.
    Date32(i32),
    /// Raw integer in the column's `TimeUnit`.
    Timestamp(i64),
}

/// Flat string column storage in Arrow layout: one contiguous byte arena +
/// offsets + validity. No per-cell allocation, and Arrow export is a block
/// copy of two buffers.
#[derive(Default)]
pub(crate) struct StrColumn {
    data: Vec<u8>,
    /// len + 1 entries; offsets[i]..offsets[i+1] is value i.
    offsets: Vec<i32>,
    validity: Vec<bool>,
}

impl StrColumn {
    fn with_capacity(cap: usize) -> Self {
        let mut offsets = Vec::with_capacity(cap + 1);
        offsets.push(0);
        StrColumn {
            data: Vec::with_capacity(cap * 16),
            offsets,
            validity: Vec::with_capacity(cap),
        }
    }

    fn push(&mut self, v: Option<&str>) {
        if let Some(s) = v {
            self.data.extend_from_slice(s.as_bytes());
        }
        self.offsets.push(self.data.len() as i32);
        self.validity.push(v.is_some());
    }

    fn pop(&mut self) {
        if self.validity.pop().is_some() {
            self.offsets.pop();
            self.data.truncate(*self.offsets.last().unwrap() as usize);
        }
    }

    fn split_off(&mut self, n: usize) -> Self {
        assert!(n <= self.len());
        if n == 0 {
            return Self::default();
        }
        if n == self.len() {
            let other = std::mem::take(self);
            self.offsets.push(0);
            return other;
        }
        // n is prefix to drain; self retains suffix
        let split_offset = self.offsets[n] as usize;
        let mut other_data = Vec::with_capacity(split_offset);
        other_data.extend_from_slice(&self.data[..split_offset]);
        let mut other_offsets = Vec::with_capacity(n + 1);
        other_offsets.extend_from_slice(&self.offsets[..=n]);
        let mut other_validity = Vec::with_capacity(n);
        other_validity.extend_from_slice(&self.validity[..n]);

        // Adjust self: drop prefix
        self.data.drain(..split_offset);
        // Rebase offsets: subtract split_offset and drop first n
        let mut new_offsets = Vec::with_capacity(self.validity.len() - n + 1);
        new_offsets.push(0);
        for &off in &self.offsets[n + 1..] {
            new_offsets.push(off - split_offset as i32);
        }
        self.offsets = new_offsets;
        self.validity.drain(..n);

        Self {
            data: other_data,
            offsets: other_offsets,
            validity: other_validity,
        }
    }

    fn len(&self) -> usize {
        self.validity.len()
    }

    fn bytes_used(&self) -> usize {
        self.data.len() + self.offsets.len() * 4 + self.validity.len()
    }

    /// Total allocated capacity in bytes (data + offsets + validity).
    fn capacity_bytes(&self) -> usize {
        self.data.capacity() + self.offsets.capacity() * 4 + self.validity.capacity()
    }

    fn get(&self, i: usize) -> Option<&str> {
        if !*self.validity.get(i)? {
            return None;
        }
        let start = self.offsets[i] as usize;
        let end = self.offsets[i + 1] as usize;
        std::str::from_utf8(&self.data[start..end]).ok()
    }

    /// Move all values from `other` onto the end of `self`.
    fn append(&mut self, other: &StrColumn) {
        let base = self.data.len() as i32;
        self.data.extend_from_slice(&other.data);
        self.offsets
            .extend(other.offsets[1..].iter().map(|o| o + base));
        self.validity.extend_from_slice(&other.validity);
    }

    fn iter(&self) -> impl Iterator<Item = Option<&str>> {
        (0..self.len()).map(move |i| self.get(i))
    }

    /// Convert to Arrow StringArray by moving buffers (zero-copy).
    /// Takes `&mut self` so we can `std::mem::take` the Vecs and preserve
    /// their capacity for reuse by the streaming path.
    fn to_arrow(&mut self) -> Result<ArrayRef> {
        use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
        // Move the Vecs out, leaving zero-capacity replacements.
        // The streaming path preserves capacity via mem::replace in batch boundaries.
        let offsets = std::mem::take(&mut self.offsets);
        let data = std::mem::take(&mut self.data);
        let validity = std::mem::take(&mut self.validity);
        let nulls = if validity.iter().all(|&v| v) {
            None
        } else {
            Some(NullBuffer::from(validity))
        };
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let data = Buffer::from_vec(data);
        let arr = StringArray::try_new(offsets, data, nulls)?;
        Ok(Arc::new(arr))
    }
}

/// Per-column builder: stores all values.  The variant determines the storage
/// type (String, Int64, Float64, Boolean, Date32, Timestamp, or Dictionary).
pub(crate) enum ColumnBuilder {
    String(StrColumn),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Boolean(Vec<Option<bool>>),
    /// Days since the Unix epoch.
    Date32(Vec<Option<i32>>),
    /// Raw integers in `unit` since the Unix epoch.
    Timestamp(TimeUnit, Vec<Option<i64>>),
    Dictionary {
        codes: Vec<Option<i32>>,
        dict: Vec<String>,
        /// value → code side-index.
        index: HashMap<String, i32>,
    },
}

/// Look up `v` in the dictionary index, inserting a new code if absent.
fn dict_code(dict: &mut Vec<String>, index: &mut HashMap<String, i32>, v: &str) -> i32 {
    if let Some(&code) = index.get(v) {
        return code;
    }
    let code = dict.len() as i32;
    dict.push(v.to_owned());
    index.insert(v.to_owned(), code);
    code
}

/// Parse an ISO-8601 date (`YYYY-MM-DD`) into days since the Unix epoch.
pub fn parse_date32(s: &str) -> Option<i32> {
    let d = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some((d - epoch).num_days() as i32)
}

/// Parse an ISO-8601 datetime (or bare date = midnight) into an integer in
/// `unit`. Naive parsing only; adapters handling timezones should emit
/// `Value::Timestamp` directly.
pub fn parse_timestamp(s: &str, unit: TimeUnit) -> Option<i64> {
    let t = s.trim();
    let dt = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| {
            chrono::NaiveDate::parse_from_str(t, "%Y-%m-%d")
                .map(|d| d.and_hms_opt(0, 0, 0).expect("midnight is valid"))
        })
        .ok()?;
    let utc = dt.and_utc();
    Some(match unit {
        TimeUnit::Second => utc.timestamp(),
        TimeUnit::Millisecond => utc.timestamp_millis(),
        TimeUnit::Microsecond => utc.timestamp_micros(),
        TimeUnit::Nanosecond => utc
            .timestamp_nanos_opt()
            .unwrap_or_else(|| utc.timestamp_micros().saturating_mul(1_000)),
    })
}

/// Format days-since-epoch back to ISO-8601 (`YYYY-MM-DD`).
fn format_date32(days: i32) -> String {
    match chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days as i64)))
    {
        Some(d) => d.format("%Y-%m-%d").to_string(),
        None => days.to_string(),
    }
}

/// Format a raw timestamp integer to ISO-8601 (`YYYY-MM-DDTHH:MM:SS(.fff…)`).
fn format_timestamp(v: i64, unit: TimeUnit) -> String {
    let (secs, subsec_nanos) = match unit {
        TimeUnit::Second => (v, 0u32),
        TimeUnit::Millisecond => (v.div_euclid(1_000), (v.rem_euclid(1_000) * 1_000_000) as u32),
        TimeUnit::Microsecond => (
            v.div_euclid(1_000_000),
            (v.rem_euclid(1_000_000) * 1_000) as u32,
        ),
        TimeUnit::Nanosecond => (v.div_euclid(1_000_000_000), v.rem_euclid(1_000_000_000) as u32),
    };
    match chrono::DateTime::from_timestamp(secs, subsec_nanos) {
        Some(dt) => dt.naive_utc().to_string(),
        None => v.to_string(),
    }
}

impl ColumnBuilder {
    pub(crate) fn with_capacity(cap: usize, field_type: &FieldType) -> Self {
        match field_type {
            FieldType::String => ColumnBuilder::String(StrColumn::with_capacity(cap)),
            FieldType::Int64 => ColumnBuilder::Int64(Vec::with_capacity(cap)),
            FieldType::Float64 => ColumnBuilder::Float64(Vec::with_capacity(cap)),
            FieldType::Boolean => ColumnBuilder::Boolean(Vec::with_capacity(cap)),
            FieldType::Date32 => ColumnBuilder::Date32(Vec::with_capacity(cap)),
            FieldType::Timestamp(unit) => {
                ColumnBuilder::Timestamp(*unit, Vec::with_capacity(cap))
            }
            FieldType::Dictionary => ColumnBuilder::Dictionary {
                codes: Vec::with_capacity(cap),
                dict: Vec::new(),
                index: HashMap::default(),
            },
        }
    }

    /// Push a logical value into the builder.
    ///
    /// * Typed builders accept their native `Value` variant directly.
    /// * `Value::Str` is parsed according to the column type; unparseable
    ///   values become `None`.
    /// * `Value::Null` always becomes `None`.
    pub(crate) fn push_value(&mut self, value: Value<'_>) {
        match value {
            Value::Null => self.push(None),
            Value::Str(s) => self.push_str(Some(s)),
            Value::Int64(i) => match self {
                ColumnBuilder::Int64(v) => v.push(Some(i)),
                ColumnBuilder::Float64(v) => v.push(Some(i as f64)),
                ColumnBuilder::String(col) => col.push(Some(&i.to_string())),
                ColumnBuilder::Dictionary { codes, dict, index } => {
                    let code = dict_code(dict, index, &i.to_string());
                    codes.push(Some(code));
                }
                _ => self.push(None),
            },
            Value::Float64(f) => match self {
                ColumnBuilder::Float64(v) => v.push(Some(f)),
                ColumnBuilder::Int64(v) => v.push(Some(f as i64)),
                ColumnBuilder::String(col) => col.push(Some(&f.to_string())),
                ColumnBuilder::Dictionary { codes, dict, index } => {
                    let code = dict_code(dict, index, &f.to_string());
                    codes.push(Some(code));
                }
                _ => self.push(None),
            },
            Value::Bool(b) => match self {
                ColumnBuilder::Boolean(v) => v.push(Some(b)),
                ColumnBuilder::String(col) => col.push(Some(&b.to_string())),
                ColumnBuilder::Dictionary { codes, dict, index } => {
                    let code = dict_code(dict, index, &b.to_string());
                    codes.push(Some(code));
                }
                _ => self.push(None),
            },
            Value::Date32(d) => match self {
                ColumnBuilder::Date32(v) => v.push(Some(d)),
                ColumnBuilder::Int64(v) => v.push(Some(d as i64)),
                ColumnBuilder::Float64(v) => v.push(Some(d as f64)),
                ColumnBuilder::String(col) => col.push(Some(&format_date32(d))),
                ColumnBuilder::Dictionary { codes, dict, index } => {
                    let code = dict_code(dict, index, &format_date32(d));
                    codes.push(Some(code));
                }
                _ => self.push(None),
            },
            Value::Timestamp(ts) => {
                // Raw integer is interpreted in the column's unit.
                let unit = self.unit();
                match self {
                    ColumnBuilder::Timestamp(_, v) => v.push(Some(ts)),
                    ColumnBuilder::Int64(v) => v.push(Some(ts)),
                    ColumnBuilder::Float64(v) => v.push(Some(ts as f64)),
                    ColumnBuilder::String(col) => match unit {
                        Some(unit) => col.push(Some(&format_timestamp(ts, unit))),
                        None => col.push(Some(&ts.to_string())),
                    },
                    ColumnBuilder::Dictionary { codes, dict, index } => {
                        let text = match unit {
                            Some(unit) => format_timestamp(ts, unit),
                            None => ts.to_string(),
                        };
                        let code = dict_code(dict, index, &text);
                        codes.push(Some(code));
                    }
                    _ => self.push(None),
                }
            }
        }
    }

    /// The `TimeUnit` carried by this builder, if any.
    fn unit(&self) -> Option<TimeUnit> {
        match self {
            ColumnBuilder::Timestamp(unit, _) => Some(*unit),
            _ => None,
        }
    }

    /// Static discriminant for cross-chunk schema consistency checks.
    pub(crate) fn variant_key(&self) -> &'static str {
        match self {
            ColumnBuilder::String(_) => "string",
            ColumnBuilder::Int64(_) => "int64",
            ColumnBuilder::Float64(_) => "float64",
            ColumnBuilder::Boolean(_) => "boolean",
            ColumnBuilder::Date32(_) => "date32",
            // Distinguish timestamp units: merging across units is an error.
            ColumnBuilder::Timestamp(unit, _) => match unit {
                TimeUnit::Second => "timestamp[s]",
                TimeUnit::Millisecond => "timestamp[ms]",
                TimeUnit::Microsecond => "timestamp[us]",
                TimeUnit::Nanosecond => "timestamp[ns]",
            },
            ColumnBuilder::Dictionary { .. } => "dictionary",
        }
    }

    /// Convert this builder in place to the storage variant identified by
    /// `target` (a [`ColumnBuilder::variant_key`] string).
    ///
    /// Supports the safe promotions accepted by [`unify_variants`] plus
    /// same-key no-ops; any other target returns [`crate::Error::Merge`].
    pub(crate) fn promote_to_variant(&mut self, target: &'static str) -> Result<()> {
        if self.variant_key() == target {
            return Ok(());
        }

        // int64 → float64 widening.
        let ints = match self {
            ColumnBuilder::Int64(v) => Some(std::mem::take(v)),
            _ => None,
        };
        if let Some(v) = ints {
            if target != "float64" {
                *self = ColumnBuilder::Int64(v);
                return Err(crate::Error::Merge(format!(
                    "cannot promote column to unified variant '{target}'"
                )));
            }
            *self = ColumnBuilder::Float64(v.into_iter().map(|o| o.map(|n| n as f64)).collect());
            return Ok(());
        }

        // string → dictionary encoding.
        let strs = match self {
            ColumnBuilder::String(v) => Some(std::mem::take(v)),
            _ => None,
        };
        if let Some(old) = strs {
            if target != "dictionary" {
                *self = ColumnBuilder::String(old);
                return Err(crate::Error::Merge(format!(
                    "cannot promote column to unified variant '{target}'"
                )));
            }
            let mut dict: Vec<String> = Vec::new();
            let mut index: HashMap<String, i32> = HashMap::default();
            let mut codes: Vec<Option<i32>> = Vec::with_capacity(old.len());
            for val in old.iter() {
                match val {
                    Some(s) => codes.push(Some(dict_code(&mut dict, &mut index, s))),
                    None => codes.push(None),
                }
            }
            *self = ColumnBuilder::Dictionary { codes, dict, index };
            return Ok(());
        }

        Err(crate::Error::Merge(format!(
            "cannot promote column to unified variant '{target}'"
        )))
    }

    /// Push an owned optional string. Kept for compatibility with code that
    /// already holds an owned value.
    pub(crate) fn push(&mut self, value: Option<String>) {
        match self {
            ColumnBuilder::String(v) => v.push(value.as_deref()),
            ColumnBuilder::Int64(v) => {
                v.push(value.and_then(|s| lexical::parse::<i64, _>(s.as_bytes()).ok()));
            }
            ColumnBuilder::Float64(v) => {
                v.push(value.and_then(|s| lexical::parse::<f64, _>(s.as_bytes()).ok()));
            }
            ColumnBuilder::Boolean(v) => {
                v.push(value.and_then(|s| s.parse::<bool>().ok()));
            }
            ColumnBuilder::Date32(v) => {
                v.push(value.and_then(|s| parse_date32(&s)));
            }
            ColumnBuilder::Timestamp(unit, v) => {
                v.push(value.and_then(|s| parse_timestamp(&s, *unit)));
            }
            ColumnBuilder::Dictionary { codes, dict, index } => match value {
                Some(v) => {
                    let idx = dict_code(dict, index, &v);
                    codes.push(Some(idx));
                }
                None => codes.push(None),
            },
        }
    }

    /// Push a borrowed string. Avoids allocation for typed columns that parse
    /// and discard the string.
    pub(crate) fn push_str(&mut self, value: Option<&str>) {
        match self {
            ColumnBuilder::String(v) => v.push(value),
            ColumnBuilder::Int64(v) => {
                v.push(value.and_then(|s| lexical::parse::<i64, _>(s.as_bytes()).ok()));
            }
            ColumnBuilder::Float64(v) => {
                v.push(value.and_then(|s| lexical::parse::<f64, _>(s.as_bytes()).ok()));
            }
            ColumnBuilder::Boolean(v) => {
                v.push(value.and_then(|s| s.parse::<bool>().ok()));
            }
            ColumnBuilder::Date32(v) => {
                v.push(value.and_then(parse_date32));
            }
            ColumnBuilder::Timestamp(unit, v) => {
                v.push(value.and_then(|s| parse_timestamp(s, *unit)));
            }
            ColumnBuilder::Dictionary { codes, dict, index } => match value {
                Some(v) => {
                    let idx = dict_code(dict, index, v);
                    codes.push(Some(idx));
                }
                None => codes.push(None),
            },
        }
    }

    pub(crate) fn pop(&mut self) {
        match self {
            ColumnBuilder::String(v) => v.pop(),
            ColumnBuilder::Int64(v) => drop(v.pop()),
            ColumnBuilder::Float64(v) => drop(v.pop()),
            ColumnBuilder::Boolean(v) => drop(v.pop()),
            ColumnBuilder::Date32(v) => drop(v.pop()),
            ColumnBuilder::Timestamp(_, v) => drop(v.pop()),
            ColumnBuilder::Dictionary { codes, .. } => drop(codes.pop()),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            ColumnBuilder::String(v) => v.len(),
            ColumnBuilder::Int64(v) => v.len(),
            ColumnBuilder::Float64(v) => v.len(),
            ColumnBuilder::Boolean(v) => v.len(),
            ColumnBuilder::Date32(v) => v.len(),
            ColumnBuilder::Timestamp(_, v) => v.len(),
            ColumnBuilder::Dictionary { codes, .. } => codes.len(),
        }
    }

    pub(crate) fn bytes_used(&self) -> usize {
        match self {
            ColumnBuilder::String(s) => s.bytes_used(),
            ColumnBuilder::Int64(v) => v.len() * 8 + v.capacity() * 0,
            ColumnBuilder::Float64(v) => v.len() * 8,
            ColumnBuilder::Boolean(v) => v.len(),
            ColumnBuilder::Date32(v) => v.len() * 4,
            ColumnBuilder::Timestamp(_, v) => v.len() * 8,
            ColumnBuilder::Dictionary { codes, dict, .. } => {
                codes.len() * 4 + dict.len() * 16 + codes.capacity() * 0
            }
        }
    }

    /// Total allocated capacity in bytes.
    pub(crate) fn capacity_bytes(&self) -> usize {
        match self {
            ColumnBuilder::String(s) => s.capacity_bytes(),
            ColumnBuilder::Int64(v) => v.capacity() * 8,
            ColumnBuilder::Float64(v) => v.capacity() * 8,
            ColumnBuilder::Boolean(v) => v.capacity(),
            ColumnBuilder::Date32(v) => v.capacity() * 4,
            ColumnBuilder::Timestamp(_, v) => v.capacity() * 8,
            ColumnBuilder::Dictionary { codes, dict, .. } => {
                codes.capacity() * 4 + dict.capacity() * 16
            }
        }
    }

    pub(crate) fn split_off(&mut self, n: usize) -> Self {
        assert!(n <= self.len());
        match self {
            ColumnBuilder::String(s) => ColumnBuilder::String(s.split_off(n)),
            ColumnBuilder::Int64(v) => {
                let other = v[..n].to_vec();
                v.drain(..n);
                ColumnBuilder::Int64(other)
            }
            ColumnBuilder::Float64(v) => {
                let other = v[..n].to_vec();
                v.drain(..n);
                ColumnBuilder::Float64(other)
            }
            ColumnBuilder::Boolean(v) => {
                let other = v[..n].to_vec();
                v.drain(..n);
                ColumnBuilder::Boolean(other)
            }
            ColumnBuilder::Date32(v) => {
                let other = v[..n].to_vec();
                v.drain(..n);
                ColumnBuilder::Date32(other)
            }
            ColumnBuilder::Timestamp(unit, v) => {
                let other = v[..n].to_vec();
                v.drain(..n);
                ColumnBuilder::Timestamp(*unit, other)
            }
            ColumnBuilder::Dictionary { codes, dict, index } => {
                let other_codes = codes[..n].to_vec();
                codes.drain(..n);
                ColumnBuilder::Dictionary {
                    codes: other_codes,
                    dict: dict.clone(),
                    index: index.clone(),
                }
            }
        }
    }

    /// Borrowed string at `index` for zero-allocation filter comparison.
    /// Returns `Some(&str)` for String and Dictionary columns only;
    /// typed columns return `None` (caller falls back to `get_filter_value`).
    pub(crate) fn get_filter_view(&self, index: usize) -> Option<&str> {
        match self {
            ColumnBuilder::String(v) => v.get(index),
            ColumnBuilder::Dictionary { codes, dict, .. } => {
                codes.get(index).and_then(|code| code.map(|idx| dict[idx as usize].as_str()))
            }
            _ => None,
        }
    }

    /// Value at `index` formatted as a string for filter comparison.
    /// Date/timestamp columns format as ISO-8601.
    pub(crate) fn get_filter_value(&self, index: usize) -> Option<String> {
        match self {
            ColumnBuilder::String(v) => v.get(index).map(|s| s.to_owned()),
            ColumnBuilder::Int64(v) => v.get(index).and_then(|o| o.map(|n| n.to_string())),
            ColumnBuilder::Float64(v) => v.get(index).and_then(|o| o.map(|n| n.to_string())),
            ColumnBuilder::Boolean(v) => v.get(index).and_then(|o| o.map(|n| n.to_string())),
            ColumnBuilder::Date32(v) => {
                v.get(index).and_then(|o| *o).map(format_date32)
            }
            ColumnBuilder::Timestamp(unit, v) => {
                let unit = *unit;
                v.get(index)
                    .and_then(|o| *o)
                    .map(|ts| format_timestamp(ts, unit))
            }
            ColumnBuilder::Dictionary { codes, dict, .. } => codes
                .get(index)
                .and_then(|code| code.map(|idx| dict[idx as usize].clone())),
        }
    }

    /// Borrowed typed value at `index` for native filter comparison.
    /// Dictionary columns decode to their string form.
    pub(crate) fn get_typed_value(&self, index: usize) -> Option<TypedValue<'_>> {
        match self {
            ColumnBuilder::String(v) => v.get(index).map(TypedValue::Str),
            ColumnBuilder::Int64(v) => v.get(index)?.map(TypedValue::Int64),
            ColumnBuilder::Float64(v) => v.get(index)?.map(TypedValue::Float64),
            ColumnBuilder::Boolean(v) => v.get(index)?.map(TypedValue::Bool),
            ColumnBuilder::Date32(v) => v.get(index)?.map(TypedValue::Date32),
            ColumnBuilder::Timestamp(_, v) => v.get(index)?.map(TypedValue::Timestamp),
            ColumnBuilder::Dictionary { codes, dict, .. } => codes
                .get(index)?
                .map(|idx| TypedValue::Str(dict[idx as usize].as_str())),
        }
    }

    /// Merge all values from `other` into `self`, consuming `other`; values are
    /// moved, never cloned. Both must be the same variant.
    pub(crate) fn extend_owned(&mut self, other: ColumnBuilder) -> Result<()> {
        match (self, other) {
            (ColumnBuilder::String(a), ColumnBuilder::String(b)) => {
                a.append(&b);
            }
            (ColumnBuilder::Int64(a), ColumnBuilder::Int64(mut b)) => {
                a.append(&mut b);
            }
            (ColumnBuilder::Float64(a), ColumnBuilder::Float64(mut b)) => {
                a.append(&mut b);
            }
            (ColumnBuilder::Boolean(a), ColumnBuilder::Boolean(mut b)) => {
                a.append(&mut b);
            }
            (ColumnBuilder::Date32(a), ColumnBuilder::Date32(mut b)) => {
                a.append(&mut b);
            }
            (ColumnBuilder::Timestamp(ua, a), ColumnBuilder::Timestamp(ub, mut b)) => {
                let unit_a: TimeUnit = *ua;
                if unit_a != ub {
                    return Err(crate::Error::Merge(format!(
                        "extend_owned: timestamp unit mismatch ({unit_a:?} vs {ub:?})"
                    )));
                }
                a.append(&mut b);
            }
            (
                ColumnBuilder::Dictionary {
                    codes: a_codes,
                    dict: a_dict,
                    index: a_index,
                },
                ColumnBuilder::Dictionary {
                    codes: b_codes,
                    dict: b_dict,
                    ..
                },
            ) => {
                // Remap b's dictionary into a's once, then translate codes.
                let remap: Vec<i32> = b_dict
                    .iter()
                    .map(|val| dict_code(a_dict, a_index, val))
                    .collect();
                a_codes.extend(b_codes.iter().map(|c| c.map(|idx| remap[idx as usize])));
            }
            _ => {
                return Err(crate::Error::Merge(
                    "extend_owned: column type mismatch across chunks".to_string(),
                ))
            }
        }
        Ok(())
    }

    /// Upgrade a String builder to Dictionary if cardinality is low enough.
    /// No-op if not String, or if rows < min_rows.
    ///
    /// `max_ratio` is the maximum fraction of rows allowed as distinct values
    /// (default `0.05`); `max_size` caps the dictionary length (default `256`).
    /// The effective threshold is clamped to at least 16 entries.
    pub(crate) fn try_upgrade_to_dict(
        &mut self,
        min_rows: usize,
        max_ratio: f64,
        max_size: usize,
    ) {
        let old = match self {
            ColumnBuilder::String(v) => std::mem::take(v),
            _ => return,
        };
        if old.len() < min_rows {
            *self = ColumnBuilder::String(old);
            return;
        }
        // Count distinct values.
        let mut seen: HashSet<&str> = HashSet::default();
        for s in old.iter().flatten() {
            seen.insert(s);
        }
        // Threshold: at most `max_ratio` of rows distinct, floored at 16 so
        // tiny columns can still upgrade, then capped by `max_size`.
        let ratio_cap = ((old.len() as f64 * max_ratio) as usize).max(16);
        let cap = ratio_cap.min(max_size);
        if seen.len() > cap {
            *self = ColumnBuilder::String(old);
            return;
        }
        // Upgrade: build dictionary + codes.
        let mut dict: Vec<String> = Vec::new();
        let mut index: HashMap<String, i32> = HashMap::default();
        let mut codes: Vec<Option<i32>> = Vec::with_capacity(old.len());
        for v in old.iter() {
            match v {
                Some(s) => {
                    let idx = dict_code(&mut dict, &mut index, s);
                    codes.push(Some(idx));
                }
                None => codes.push(None),
            }
        }
        *self = ColumnBuilder::Dictionary { codes, dict, index };
    }

    /// Arrow logical type for this column.
    pub(crate) fn arrow_datatype(&self) -> DataType {
        match self {
            ColumnBuilder::String(_) => DataType::Utf8,
            ColumnBuilder::Int64(_) => DataType::Int64,
            ColumnBuilder::Float64(_) => DataType::Float64,
            ColumnBuilder::Boolean(_) => DataType::Boolean,
            ColumnBuilder::Date32(_) => DataType::Date32,
            ColumnBuilder::Timestamp(unit, _) => DataType::Timestamp(*unit, None),
            ColumnBuilder::Dictionary { .. } => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
        }
    }

    /// Build a native Arrow array from the column builder.
    /// Build a native Arrow array from the column builder.
    /// Takes `&mut self` to move buffers instead of copying (zero-copy export).
    pub(crate) fn to_arrow_array(&mut self) -> Result<ArrayRef> {
        // Handle empty columns (after mem::take from a previous export).
        if self.len() == 0 {
            return Ok(match self {
                ColumnBuilder::String(_) => {
                    Arc::new(StringArray::from(vec![None::<&str>; 0]))
                }
                ColumnBuilder::Int64(_) => Arc::new(Int64Array::from(vec![None::<i64>; 0])),
                ColumnBuilder::Float64(_) => Arc::new(Float64Array::from(vec![None::<f64>; 0])),
                ColumnBuilder::Boolean(_) => Arc::new(BooleanArray::from(vec![None::<bool>; 0])),
                ColumnBuilder::Date32(_) => Arc::new(Date32Array::from(vec![None::<i32>; 0])),
                ColumnBuilder::Timestamp(unit, _) => match unit {
                    TimeUnit::Second => Arc::new(PrimitiveArray::<TimestampSecondType>::from(vec![None::<i64>; 0])),
                    TimeUnit::Millisecond => Arc::new(PrimitiveArray::<TimestampMillisecondType>::from(vec![None::<i64>; 0])),
                    TimeUnit::Microsecond => Arc::new(PrimitiveArray::<TimestampMicrosecondType>::from(vec![None::<i64>; 0])),
                    TimeUnit::Nanosecond => Arc::new(PrimitiveArray::<TimestampNanosecondType>::from(vec![None::<i64>; 0])),
                },
                ColumnBuilder::Dictionary { .. } => {
                    let keys: Int32Array = vec![None::<i32>; 0].into_iter().collect();
                    let values: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; 0]));
                    Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values)?)
                }
            });
        }
        Ok(match self {
            ColumnBuilder::String(v) => v.to_arrow()?,
            ColumnBuilder::Int64(v) => {
                let vals = std::mem::take(v);
                Arc::new(vals.into_iter().collect::<Int64Array>())
            }
            ColumnBuilder::Float64(v) => {
                let vals = std::mem::take(v);
                Arc::new(vals.into_iter().collect::<Float64Array>())
            }
            ColumnBuilder::Boolean(v) => {
                let vals = std::mem::take(v);
                Arc::new(vals.into_iter().collect::<BooleanArray>())
            }
            ColumnBuilder::Date32(v) => {
                let vals = std::mem::take(v);
                Arc::new(vals.into_iter().collect::<Date32Array>())
            }
            ColumnBuilder::Timestamp(unit, v) => {
                let vals = std::mem::take(v);
                match unit {
                    TimeUnit::Second => Arc::new(vals.into_iter().collect::<PrimitiveArray<TimestampSecondType>>()),
                    TimeUnit::Millisecond => Arc::new(vals.into_iter().collect::<PrimitiveArray<TimestampMillisecondType>>()),
                    TimeUnit::Microsecond => Arc::new(vals.into_iter().collect::<PrimitiveArray<TimestampMicrosecondType>>()),
                    TimeUnit::Nanosecond => Arc::new(vals.into_iter().collect::<PrimitiveArray<TimestampNanosecondType>>()),
                }
            }
            ColumnBuilder::Dictionary { codes, dict, .. } => {
                let codes_arr: Int32Array = std::mem::take(codes).into_iter().collect();
                let values: ArrayRef = Arc::new(
                    dict.iter()
                        .map(|s| Some(s.as_str()))
                        .collect::<StringArray>(),
                );
                let arr = DictionaryArray::<Int32Type>::try_new(codes_arr, values)?;
                Arc::new(arr)
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn as_str_vec(&self) -> Vec<Option<String>> {
        match self {
            ColumnBuilder::String(v) => v.iter().map(|o| o.map(str::to_owned)).collect(),
            _ => panic!("as_str_vec called on non-String ColumnBuilder"),
        }
    }
}

/// Every string produced by [`ColumnBuilder::variant_key`].
const VARIANT_KEYS: [&str; 10] = [
    "string",
    "int64",
    "float64",
    "boolean",
    "date32",
    "timestamp[s]",
    "timestamp[ms]",
    "timestamp[us]",
    "timestamp[ns]",
    "dictionary",
];

/// Reconcile two variant keys into a single storage variant.
///
/// Returns the unified key, or `None` when the types are irreconcilable and
/// callers must surface an error. Safe promotions:
///
/// - `int64` + `float64` → `float64`
/// - `string` + `dictionary` → `dictionary`
pub(crate) fn unify_variants(a: &str, b: &str) -> Option<&'static str> {
    if a == b {
        return VARIANT_KEYS.iter().copied().find(|k| *k == a);
    }
    match (a, b) {
        ("int64", "float64") | ("float64", "int64") => Some("float64"),
        ("string", "dictionary") | ("dictionary", "string") => Some("dictionary"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::FieldType;
    use crate::value::Value;

    #[test]
    fn test_push_value_str_parsing() {
        let mut b = ColumnBuilder::with_capacity(4, &FieldType::Int64);
        b.push_value(Value::Str("42"));
        b.push_value(Value::Str("bad"));
        b.push_value(Value::Null);
        if let ColumnBuilder::Int64(v) = &b {
            assert_eq!(v, &vec![Some(42), None, None]);
        } else {
            panic!("expected Int64");
        }
    }

    #[test]
    fn test_push_value_native_types() {
        let mut b = ColumnBuilder::with_capacity(4, &FieldType::Int64);
        b.push_value(Value::Int64(7));
        b.push_value(Value::Float64(3.5)); // widened to i64
        if let ColumnBuilder::Int64(v) = &b {
            assert_eq!(v[0], Some(7));
            assert_eq!(v[1], Some(3));
        } else {
            panic!("expected Int64");
        }

        let mut f = ColumnBuilder::with_capacity(2, &FieldType::Float64);
        f.push_value(Value::Float64(2.5));
        f.push_value(Value::Int64(1));
        if let ColumnBuilder::Float64(v) = &f {
            assert!((v[0].unwrap() - 2.5).abs() < 1e-9);
            assert!((v[1].unwrap() - 1.0).abs() < 1e-9);
        } else {
            panic!("expected Float64");
        }
    }

    #[test]
    fn test_push_value_to_string_column() {
        let mut b = ColumnBuilder::with_capacity(3, &FieldType::String);
        b.push_value(Value::Int64(42));
        b.push_value(Value::Bool(true));
        b.push_value(Value::Null);
        assert_eq!(
            b.as_str_vec(),
            vec![Some("42".into()), Some("true".into()), None]
        );
    }

    #[test]
    fn test_all_value_variants_by_column_type() {
        // Int64 column: native Int64, Float64 widened, Bool becomes null.
        let mut i = ColumnBuilder::with_capacity(4, &FieldType::Int64);
        i.push_value(Value::Int64(7));
        i.push_value(Value::Float64(3.9)); // truncated to 3
        i.push_value(Value::Bool(true)); // unsupported -> null
        i.push_value(Value::Null);
        if let ColumnBuilder::Int64(v) = &i {
            assert_eq!(v, &vec![Some(7), Some(3), None, None]);
        } else {
            panic!("expected Int64 builder");
        }

        // Float64 column: native Float64, Int64 widened, Bool becomes null.
        let mut f = ColumnBuilder::with_capacity(3, &FieldType::Float64);
        f.push_value(Value::Float64(2.5));
        f.push_value(Value::Int64(1));
        f.push_value(Value::Bool(false));
        if let ColumnBuilder::Float64(v) = &f {
            assert!((v[0].unwrap() - 2.5).abs() < 1e-9);
            assert!((v[1].unwrap() - 1.0).abs() < 1e-9);
            assert_eq!(v[2], None);
        } else {
            panic!("expected Float64 builder");
        }

        // Boolean column: native Bool, Str parsed, Int64 becomes null.
        let mut b = ColumnBuilder::with_capacity(4, &FieldType::Boolean);
        b.push_value(Value::Bool(true));
        b.push_value(Value::Str("false"));
        b.push_value(Value::Str("not_bool"));
        b.push_value(Value::Int64(1));
        if let ColumnBuilder::Boolean(v) = &b {
            assert_eq!(v, &vec![Some(true), Some(false), None, None]);
        } else {
            panic!("expected Boolean builder");
        }

        // String column: every variant is formatted as text.
        let mut s = ColumnBuilder::with_capacity(4, &FieldType::String);
        s.push_value(Value::Int64(42));
        s.push_value(Value::Float64(2.5));
        s.push_value(Value::Bool(false));
        s.push_value(Value::Null);
        assert_eq!(
            s.as_str_vec(),
            vec![
                Some("42".into()),
                Some("2.5".into()),
                Some("false".into()),
                None
            ]
        );

        // Dictionary column: every variant is formatted as text and encoded.
        let mut d = ColumnBuilder::with_capacity(4, &FieldType::Dictionary);
        d.push_value(Value::Int64(7));
        d.push_value(Value::Float64(7.0)); // same string as Int64(7)
        d.push_value(Value::Bool(true));
        d.push_value(Value::Null);
        if let ColumnBuilder::Dictionary { codes, dict, .. } = &d {
            assert_eq!(dict, &["7", "true"]);
            assert_eq!(codes, &vec![Some(0), Some(0), Some(1), None]);
        } else {
            panic!("expected Dictionary builder");
        }
    }
}
