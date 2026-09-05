#[cfg(test)]
use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, DictionaryArray, Float64Array, Int32Array, Int64Array,
    PrimitiveArray, StringArray,
};
use arrow::datatypes::{
    DataType, Int32Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use rustc_hash::FxHashSet as HashSet;

use crate::plan::FieldType;
use crate::value::Value;
use crate::Result;

/// Compact one-bit-per-row validity storage.
#[derive(Default, Debug, Clone)]
struct ValidityBitmap {
    bits: Vec<u8>,
    len: usize,
    null_count: usize,
}

impl ValidityBitmap {
    fn with_capacity(cap: usize) -> Self {
        Self {
            bits: Vec::with_capacity(cap.div_ceil(8)),
            len: 0,
            null_count: 0,
        }
    }

    fn push(&mut self, valid: bool) {
        if self.len.is_multiple_of(8) {
            self.bits.push(0);
        }
        if valid {
            self.bits[self.len / 8] |= 1 << (self.len % 8);
        } else {
            self.null_count += 1;
        }
        self.len += 1;
    }

    fn pop(&mut self) -> Option<bool> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        let byte = &mut self.bits[self.len / 8];
        let valid = (*byte & (1 << (self.len % 8))) != 0;
        if !valid {
            self.null_count -= 1;
        }
        *byte &= !(1 << (self.len % 8));
        if self.len.is_multiple_of(8) {
            self.bits.pop();
        }
        Some(valid)
    }

    fn is_valid(&self, i: usize) -> bool {
        i < self.len && (self.bits[i / 8] & (1 << (i % 8))) != 0
    }

    fn len(&self) -> usize {
        self.len
    }
    fn bytes_used(&self) -> usize {
        self.bits.len()
    }
    fn capacity_bytes(&self) -> usize {
        self.bits.capacity()
    }

    fn append(&mut self, other: &Self) {
        if self.len.is_multiple_of(8) {
            self.bits.extend_from_slice(&other.bits);
            self.len += other.len;
            self.null_count += other.null_count;
            if !self.len.is_multiple_of(8) {
                let mask = (1u8 << (self.len % 8)) - 1;
                if let Some(last) = self.bits.last_mut() {
                    *last &= mask;
                }
            }
            return;
        }
        for i in 0..other.len {
            self.push(other.is_valid(i));
        }
    }

    fn into_arrow(self) -> Option<arrow::buffer::NullBuffer> {
        if self.null_count == 0 {
            return None;
        }
        let buffer = arrow::buffer::BooleanBuffer::new(
            arrow::buffer::Buffer::from_vec(self.bits),
            0,
            self.len,
        );
        Some(arrow::buffer::NullBuffer::new(buffer))
    }

    fn split_off(&mut self, n: usize) -> Self {
        let mut other = Self::with_capacity(n);
        for i in 0..n {
            other.push(self.is_valid(i));
        }
        let remaining: Vec<bool> = (n..self.len).map(|i| self.is_valid(i)).collect();
        *self = Self::with_capacity(remaining.len());
        for valid in remaining {
            self.push(valid);
        }
        other
    }
}

#[derive(Default, Debug)]
pub(crate) struct NullableColumn<T> {
    values: Vec<T>,
    validity: ValidityBitmap,
}

impl<T: Default + Clone> NullableColumn<T> {
    fn with_capacity(cap: usize) -> Self {
        Self {
            values: Vec::with_capacity(cap),
            validity: ValidityBitmap::with_capacity(cap),
        }
    }

    fn push(&mut self, value: Option<T>) {
        let valid = value.is_some();
        self.values.push(value.unwrap_or_default());
        self.validity.push(valid);
    }

    fn pop(&mut self) -> Option<T> {
        self.validity.pop()?;
        self.values.pop()
    }

    fn len(&self) -> usize {
        self.validity.len()
    }
    pub(crate) fn get(&self, i: usize) -> Option<&T> {
        self.validity.is_valid(i).then(|| &self.values[i])
    }
    fn capacity_bytes(&self) -> usize {
        self.values.capacity() * std::mem::size_of::<T>() + self.validity.capacity_bytes()
    }
    fn split_off(&mut self, n: usize) -> Self {
        let values = self.values[..n].to_vec();
        self.values.drain(..n);
        let validity = self.validity.split_off(n);
        Self { values, validity }
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = Option<&T>> {
        (0..self.len()).map(|i| self.get(i))
    }
}

impl<T: Default + Clone> NullableColumn<T> {
    fn into_options(self) -> impl Iterator<Item = Option<T>> {
        let validity = self.validity;
        self.values
            .into_iter()
            .enumerate()
            .map(move |(i, value)| validity.is_valid(i).then_some(value))
    }
}

impl NullableColumn<i32> {
    /// Remap dictionary codes in place using a pre-built map.
    /// Null entries are skipped; valid entries have `values[i] = map[values[i]]`.
    pub(crate) fn remap_codes(&mut self, map: &[i32]) {
        for i in 0..self.len() {
            if self.validity.is_valid(i) {
                let v = &mut self.values[i];
                debug_assert!((*v as usize) < map.len(), "code out of bounds in remap");
                unsafe {
                    *v = *map.get_unchecked(*v as usize);
                }
            }
        }
    }
}

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
    validity: ValidityBitmap,
}

impl StrColumn {
    fn with_capacity(cap: usize) -> Self {
        let mut offsets = Vec::with_capacity(cap + 1);
        offsets.push(0);
        StrColumn {
            data: Vec::with_capacity(cap * 16),
            offsets,
            validity: ValidityBitmap::with_capacity(cap),
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
        let other_validity = self.validity.split_off(n);

        // Adjust self: drop prefix
        self.data.drain(..split_offset);
        // Rebase offsets: subtract split_offset and drop first n
        let mut new_offsets = Vec::with_capacity(self.validity.len() + 1);
        new_offsets.push(0);
        for &off in &self.offsets[n + 1..] {
            new_offsets.push(off - split_offset as i32);
        }
        self.offsets = new_offsets;

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
        self.data.len() + self.offsets.len() * 4 + self.validity.bytes_used()
    }

    /// Total allocated capacity in bytes (data + offsets + validity).
    fn capacity_bytes(&self) -> usize {
        self.data.capacity() + self.offsets.capacity() * 4 + self.validity.capacity_bytes()
    }

    fn get(&self, i: usize) -> Option<&str> {
        if !self.validity.is_valid(i) {
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
        self.validity.append(&other.validity);
    }

    fn iter(&self) -> impl Iterator<Item = Option<&str>> {
        (0..self.len()).map(move |i| self.get(i))
    }

    /// Convert to Arrow StringArray by moving buffers (zero-copy).
    /// Takes `&mut self` so we can `std::mem::take` the Vecs and preserve
    /// their capacity for reuse by the streaming path.
    #[allow(
        clippy::wrong_self_convention,
        reason = "takes &mut self for zero-copy mem::take"
    )]
    fn to_arrow(&mut self) -> Result<ArrayRef> {
        use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
        // Move the Vecs out, leaving zero-capacity replacements.
        // The streaming path preserves capacity via mem::replace in batch boundaries.
        let offsets = std::mem::take(&mut self.offsets);
        let data = std::mem::take(&mut self.data);
        let validity = std::mem::take(&mut self.validity);
        let nulls = validity.into_arrow();
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let data = Buffer::from_vec(data);
        let arr = StringArray::try_new(offsets, data, nulls)?;
        Ok(Arc::new(arr))
    }
}

/// Flat primitive-column storage: one contiguous data array + a validity
/// bitmap (one bit per row). Mirrors [`StrColumn`] layout but for `Copy` types.
#[derive(Clone)]
pub(crate) struct PrimColumn<T: Copy> {
    data: Vec<T>,
    validity: ValidityBitmap,
}

impl<T: Copy + Default> PrimColumn<T> {
    fn with_capacity(cap: usize) -> Self {
        PrimColumn {
            data: Vec::with_capacity(cap),
            validity: ValidityBitmap::with_capacity(cap),
        }
    }

    fn push(&mut self, v: Option<T>) {
        self.data.push(v.unwrap_or_default());
        self.validity.push(v.is_some());
    }

    fn pop(&mut self) {
        self.data.pop();
        self.validity.pop();
    }

    fn len(&self) -> usize {
        self.validity.len()
    }

    fn split_off(&mut self, n: usize) -> Self {
        assert!(n <= self.len());
        let other_data = self.data[..n].to_vec();
        self.data.drain(..n);
        let other_validity = self.validity.split_off(n);
        PrimColumn {
            data: other_data,
            validity: other_validity,
        }
    }

    pub(crate) fn get(&self, i: usize) -> Option<T> {
        if self.validity.is_valid(i) {
            Some(self.data[i])
        } else {
            None
        }
    }

    /// Move all values from `other` onto the end of `self`.
    fn append(&mut self, other: &PrimColumn<T>) {
        self.data.extend_from_slice(&other.data);
        self.validity.append(&other.validity);
    }
}

impl<T: Copy + Default> Default for PrimColumn<T> {
    fn default() -> Self {
        PrimColumn {
            data: Vec::new(),
            validity: ValidityBitmap::default(),
        }
    }
}

impl<T: Copy> PrimColumn<T> {
    fn bytes_used(&self) -> usize {
        self.data.len() * std::mem::size_of::<T>() + self.validity.bytes_used()
    }

    fn capacity_bytes(&self) -> usize {
        self.data.capacity() * std::mem::size_of::<T>() + self.validity.capacity_bytes()
    }

    /// Zero-copy Arrow export: moves data and validity buffers into a
    /// `PrimitiveArray` via `ScalarBuffer` and `NullBuffer`.
    #[allow(clippy::wrong_self_convention)]
    fn to_arrow<A: arrow::array::ArrowPrimitiveType>(&mut self) -> Result<ArrayRef>
    where
        A::Native: From<T>,
    {
        use arrow::buffer::ScalarBuffer;
        let data = std::mem::take(&mut self.data);
        let validity = std::mem::take(&mut self.validity);
        let nulls = validity.into_arrow();
        let buf: ScalarBuffer<A::Native> = data.into_iter().map(A::Native::from).collect();
        let arr = PrimitiveArray::<A>::try_new(buf, nulls)?;
        Ok(Arc::new(arr))
    }
}

impl PrimColumn<bool> {
    /// Zero-copy Arrow export for booleans: packs `Vec<bool>` into a
    /// `BooleanBuffer` for `BooleanArray`.
    #[allow(clippy::wrong_self_convention)]
    fn to_arrow_bool(&mut self) -> Result<ArrayRef> {
        use arrow::buffer::{BooleanBuffer, ScalarBuffer};
        let data = std::mem::take(&mut self.data);
        let validity = std::mem::take(&mut self.validity);
        let nulls = validity.into_arrow();
        let buf: ScalarBuffer<u8> = data.iter().map(|&b| b as u8).collect();
        let bool_buf = BooleanBuffer::new(buf.into(), 0, data.len());
        let arr = BooleanArray::new(bool_buf, nulls);
        Ok(Arc::new(arr))
    }
}

/// Per-column builder: stores all values.  The variant determines the storage
/// type (String, Int64, Float64, Boolean, Date32, Timestamp, or Dictionary).
pub(crate) enum ColumnBuilder {
    String(StrColumn),
    Int64(PrimColumn<i64>),
    Float64(PrimColumn<f64>),
    Boolean(PrimColumn<bool>),
    /// Days since the Unix epoch.
    Date32(PrimColumn<i32>),
    /// Raw integers in `unit` since the Unix epoch.
    Timestamp(TimeUnit, PrimColumn<i64>),
    /// Fixed-precision decimal: value * 10^scale stored as i128.
    Decimal128(u8, PrimColumn<i128>),
    Dictionary {
        codes: NullableColumn<i32>,
        /// Contiguous byte buffer for dictionary values (like StrColumn).
        data: Vec<u8>,
        /// Byte offsets into `data` for each dictionary entry.
        offsets: Vec<i32>,
        /// value → code side-index.
        index: rustc_hash::FxHashMap<Box<str>, i32>,
    },
}

/// Look up `v` in the dictionary index, inserting a new code if absent.
/// Uses a contiguous byte buffer for values instead of Vec<String>.
fn dict_code(
    data: &mut Vec<u8>,
    offsets: &mut Vec<i32>,
    index: &mut rustc_hash::FxHashMap<Box<str>, i32>,
    v: &str,
) -> i32 {
    let key: Box<str> = v.into();
    if let Some(&code) = index.get(&key) {
        return code;
    }
    let code = offsets.len() as i32 - 1;
    data.extend_from_slice(v.as_bytes());
    offsets.push(data.len() as i32);
    index.insert(key, code);
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
        TimeUnit::Millisecond => (
            v.div_euclid(1_000),
            (v.rem_euclid(1_000) * 1_000_000) as u32,
        ),
        TimeUnit::Microsecond => (
            v.div_euclid(1_000_000),
            (v.rem_euclid(1_000_000) * 1_000) as u32,
        ),
        TimeUnit::Nanosecond => (
            v.div_euclid(1_000_000_000),
            v.rem_euclid(1_000_000_000) as u32,
        ),
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
            FieldType::Int64 => ColumnBuilder::Int64(PrimColumn::with_capacity(cap)),
            FieldType::Float64 => ColumnBuilder::Float64(PrimColumn::with_capacity(cap)),
            FieldType::Boolean => ColumnBuilder::Boolean(PrimColumn::with_capacity(cap)),
            FieldType::Date32 => ColumnBuilder::Date32(PrimColumn::with_capacity(cap)),
            FieldType::Timestamp(unit) => {
                ColumnBuilder::Timestamp(*unit, PrimColumn::with_capacity(cap))
            }
            FieldType::Decimal128(scale) => {
                ColumnBuilder::Decimal128(*scale, PrimColumn::with_capacity(cap))
            }
            FieldType::Dictionary => ColumnBuilder::Dictionary {
                codes: NullableColumn::with_capacity(cap),
                data: Vec::new(),
                offsets: vec![0],
                index: rustc_hash::FxHashMap::default(),
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
            Value::Str(ref s) => self.push_str(Some(s.as_ref())),
            Value::Int64(i) => match self {
                ColumnBuilder::Int64(v) => v.push(Some(i)),
                ColumnBuilder::Float64(v) => v.push(Some(i as f64)),
                ColumnBuilder::String(col) => col.push(Some(&i.to_string())),
                ColumnBuilder::Dictionary {
                    codes,
                    data,
                    offsets,
                    index,
                } => {
                    let code = dict_code(data, offsets, index, &i.to_string());
                    codes.push(Some(code));
                }
                _ => self.push(None),
            },
            Value::Float64(f) => match self {
                ColumnBuilder::Float64(v) => v.push(Some(f)),
                ColumnBuilder::Int64(v) => v.push(Some(f as i64)),
                ColumnBuilder::String(col) => col.push(Some(&f.to_string())),
                ColumnBuilder::Dictionary {
                    codes,
                    data,
                    offsets,
                    index,
                } => {
                    let code = dict_code(data, offsets, index, &f.to_string());
                    codes.push(Some(code));
                }
                _ => self.push(None),
            },
            Value::Bool(b) => match self {
                ColumnBuilder::Boolean(v) => v.push(Some(b)),
                ColumnBuilder::String(col) => col.push(Some(&b.to_string())),
                ColumnBuilder::Dictionary {
                    codes,
                    data,
                    offsets,
                    index,
                } => {
                    let code = dict_code(data, offsets, index, &b.to_string());
                    codes.push(Some(code));
                }
                _ => self.push(None),
            },
            Value::Date32(d) => match self {
                ColumnBuilder::Date32(v) => v.push(Some(d)),
                ColumnBuilder::Int64(v) => v.push(Some(d as i64)),
                ColumnBuilder::Float64(v) => v.push(Some(d as f64)),
                ColumnBuilder::String(col) => col.push(Some(&format_date32(d))),
                ColumnBuilder::Dictionary {
                    codes,
                    data,
                    offsets,
                    index,
                } => {
                    let code = dict_code(data, offsets, index, &format_date32(d));
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
                    ColumnBuilder::Dictionary {
                        codes,
                        data,
                        offsets,
                        index,
                    } => {
                        let text = match unit {
                            Some(unit) => format_timestamp(ts, unit),
                            None => ts.to_string(),
                        };
                        let code = dict_code(data, offsets, index, &text);
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
            ColumnBuilder::Decimal128(scale, _) => {
                // Use a static string for the variant key
                match scale {
                    0 => "decimal128(0)",
                    2 => "decimal128(2)",
                    4 => "decimal128(4)",
                    8 => "decimal128(8)",
                    10 => "decimal128(10)",
                    18 => "decimal128(18)",
                    _ => "decimal128",
                }
            }
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
            let float_data: Vec<f64> = v.data.into_iter().map(|n| n as f64).collect();
            *self = ColumnBuilder::Float64(PrimColumn {
                data: float_data,
                validity: v.validity,
            });
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
            let mut data: Vec<u8> = Vec::new();
            let mut offsets: Vec<i32> = vec![0];
            let mut index: rustc_hash::FxHashMap<Box<str>, i32> = rustc_hash::FxHashMap::default();
            let mut codes = NullableColumn::with_capacity(old.len());
            for val in old.iter() {
                match val {
                    Some(s) => codes.push(Some(dict_code(&mut data, &mut offsets, &mut index, s))),
                    None => codes.push(None),
                }
            }
            *self = ColumnBuilder::Dictionary {
                codes,
                data,
                offsets,
                index,
            };
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
            ColumnBuilder::Decimal128(scale, v) => {
                v.push(value.and_then(|s| {
                    s.trim().parse::<i128>().ok().map(|n| n * 10i128.pow(*scale as u32))
                }));
            }
            ColumnBuilder::Dictionary {
                codes,
                data,
                offsets,
                index,
            } => match value {
                Some(v) => {
                    let idx = dict_code(data, offsets, index, &v);
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
            ColumnBuilder::Decimal128(scale, v) => {
                v.push(value.and_then(|s| {
                    s.trim().parse::<i128>().ok().map(|n| n * 10i128.pow(*scale as u32))
                }));
            }
            ColumnBuilder::Dictionary {
                codes,
                data,
                offsets,
                index,
            } => match value {
                Some(v) => {
                    let idx = dict_code(data, offsets, index, v);
                    codes.push(Some(idx));
                }
                None => codes.push(None),
            },
        }
    }

    pub(crate) fn pop(&mut self) {
        match self {
            ColumnBuilder::String(v) => v.pop(),
            ColumnBuilder::Int64(v) => v.pop(),
            ColumnBuilder::Float64(v) => v.pop(),
            ColumnBuilder::Boolean(v) => v.pop(),
            ColumnBuilder::Date32(v) => v.pop(),
            ColumnBuilder::Timestamp(_, v) => v.pop(),
            ColumnBuilder::Decimal128(_, v) => v.pop(),
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
            ColumnBuilder::Decimal128(_, v) => v.len(),
            ColumnBuilder::Dictionary { codes, .. } => codes.len(),
        }
    }

    pub(crate) fn bytes_used(&self) -> usize {
        match self {
            ColumnBuilder::String(s) => s.bytes_used(),
            ColumnBuilder::Int64(v) => v.bytes_used(),
            ColumnBuilder::Float64(v) => v.bytes_used(),
            ColumnBuilder::Boolean(v) => v.bytes_used(),
            ColumnBuilder::Date32(v) => v.bytes_used(),
            ColumnBuilder::Timestamp(_, v) => v.bytes_used(),
            ColumnBuilder::Decimal128(_, v) => v.bytes_used(),
            ColumnBuilder::Dictionary {
                codes,
                data,
                offsets,
                ..
            } => codes.len() * 4 + data.len() + offsets.len() * 4,
        }
    }

    /// Total allocated capacity in bytes.
    pub(crate) fn capacity_bytes(&self) -> usize {
        match self {
            ColumnBuilder::String(s) => s.capacity_bytes(),
            ColumnBuilder::Int64(v) => v.capacity_bytes(),
            ColumnBuilder::Float64(v) => v.capacity_bytes(),
            ColumnBuilder::Boolean(v) => v.capacity_bytes(),
            ColumnBuilder::Date32(v) => v.capacity_bytes(),
            ColumnBuilder::Timestamp(_, v) => v.capacity_bytes(),
            ColumnBuilder::Decimal128(_, v) => v.capacity_bytes(),
            ColumnBuilder::Dictionary {
                codes,
                data,
                offsets,
                ..
            } => codes.capacity_bytes() + data.capacity() + offsets.capacity() * 4,
        }
    }

    pub(crate) fn split_off(&mut self, n: usize) -> Self {
        assert!(n <= self.len());
        match self {
            ColumnBuilder::String(s) => ColumnBuilder::String(s.split_off(n)),
            ColumnBuilder::Int64(v) => ColumnBuilder::Int64(v.split_off(n)),
            ColumnBuilder::Float64(v) => ColumnBuilder::Float64(v.split_off(n)),
            ColumnBuilder::Boolean(v) => ColumnBuilder::Boolean(v.split_off(n)),
            ColumnBuilder::Date32(v) => ColumnBuilder::Date32(v.split_off(n)),
            ColumnBuilder::Timestamp(unit, v) => ColumnBuilder::Timestamp(*unit, v.split_off(n)),
            ColumnBuilder::Decimal128(scale, v) => ColumnBuilder::Decimal128(*scale, v.split_off(n)),
            ColumnBuilder::Dictionary {
                codes,
                data,
                offsets,
                index,
            } => {
                let other_codes = codes.split_off(n);
                ColumnBuilder::Dictionary {
                    codes: other_codes,
                    data: data.clone(),
                    offsets: offsets.clone(),
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
            ColumnBuilder::Dictionary {
                codes,
                data,
                offsets,
                ..
            } => codes.get(index).map(|code| {
                let start = offsets[*code as usize] as usize;
                let end = offsets[*code as usize + 1] as usize;
                std::str::from_utf8(&data[start..end]).unwrap_or("")
            }),
            _ => None,
        }
    }

    /// Value at `index` formatted as a string for filter comparison.
    /// Date/timestamp columns format as ISO-8601.
    pub(crate) fn get_filter_value(&self, index: usize) -> Option<String> {
        match self {
            ColumnBuilder::String(v) => v.get(index).map(|s| s.to_owned()),
            ColumnBuilder::Int64(v) => v.get(index).map(|n| n.to_string()),
            ColumnBuilder::Float64(v) => v.get(index).map(|n| n.to_string()),
            ColumnBuilder::Boolean(v) => v.get(index).map(|n| n.to_string()),
            ColumnBuilder::Date32(v) => v.get(index).map(format_date32),
            ColumnBuilder::Timestamp(unit, v) => {
                let unit = *unit;
                v.get(index).map(|ts| format_timestamp(ts, unit))
            }
            ColumnBuilder::Decimal128(_, v) => v.get(index).map(|n| n.to_string()),
            ColumnBuilder::Dictionary {
                codes,
                data,
                offsets,
                ..
            } => codes.get(index).map(|code| {
                let start = offsets[*code as usize] as usize;
                let end = offsets[*code as usize + 1] as usize;
                String::from_utf8_lossy(&data[start..end]).into_owned()
            }),
        }
    }

    /// Borrowed typed value at `index` for native filter comparison.
    /// Dictionary columns decode to their string form.
    pub(crate) fn get_typed_value(&self, index: usize) -> Option<TypedValue<'_>> {
        match self {
            ColumnBuilder::String(v) => v.get(index).map(TypedValue::Str),
            ColumnBuilder::Int64(v) => v.get(index).map(TypedValue::Int64),
            ColumnBuilder::Float64(v) => v.get(index).map(TypedValue::Float64),
            ColumnBuilder::Boolean(v) => v.get(index).map(TypedValue::Bool),
            ColumnBuilder::Date32(v) => v.get(index).map(TypedValue::Date32),
            ColumnBuilder::Timestamp(_, v) => v.get(index).map(TypedValue::Timestamp),
            ColumnBuilder::Decimal128(_, v) => v.get(index).map(|n| TypedValue::Int64(n as i64)),
            ColumnBuilder::Dictionary {
                codes,
                data,
                offsets,
                ..
            } => codes.get(index).map(|&idx| {
                let start = offsets[idx as usize] as usize;
                let end = offsets[idx as usize + 1] as usize;
                TypedValue::Str(std::str::from_utf8(&data[start..end]).unwrap_or(""))
            }),
        }
    }

    /// Merge all values from `other` into `self`, consuming `other`; values are
    /// moved, never cloned. Both must be the same variant.
    pub(crate) fn extend_owned(&mut self, other: ColumnBuilder) -> Result<()> {
        match (self, other) {
            (ColumnBuilder::String(a), ColumnBuilder::String(b)) => {
                a.append(&b);
            }
            (ColumnBuilder::Int64(a), ColumnBuilder::Int64(b)) => {
                a.append(&b);
            }
            (ColumnBuilder::Float64(a), ColumnBuilder::Float64(b)) => {
                a.append(&b);
            }
            (ColumnBuilder::Boolean(a), ColumnBuilder::Boolean(b)) => {
                a.append(&b);
            }
            (ColumnBuilder::Date32(a), ColumnBuilder::Date32(b)) => {
                a.append(&b);
            }
            (ColumnBuilder::Timestamp(ua, a), ColumnBuilder::Timestamp(ub, b)) => {
                let unit_a: TimeUnit = *ua;
                if unit_a != ub {
                    return Err(crate::Error::Merge(format!(
                        "extend_owned: timestamp unit mismatch ({unit_a:?} vs {ub:?})"
                    )));
                }
                a.append(&b);
            }
            (ColumnBuilder::Decimal128(sa, a), ColumnBuilder::Decimal128(sb, b)) => {
                if *sa != sb {
                    return Err(crate::Error::Merge(format!(
                        "extend_owned: decimal128 scale mismatch ({sa} vs {sb})"
                    )));
                }
                a.append(&b);
            }
            (
                ColumnBuilder::Dictionary {
                    codes: a_codes,
                    data: a_data,
                    offsets: a_offsets,
                    index: a_index,
                },
                ColumnBuilder::Dictionary {
                    codes: b_codes,
                    data: b_data,
                    offsets: b_offsets,
                    ..
                },
            ) => {
                // Remap b's dictionary into a's once, then translate codes.
                let mut remap: Vec<i32> = Vec::with_capacity(b_offsets.len() - 1);
                for i in 0..(b_offsets.len() - 1) {
                    let start = b_offsets[i] as usize;
                    let end = b_offsets[i + 1] as usize;
                    let val = std::str::from_utf8(&b_data[start..end]).unwrap_or("");
                    remap.push(dict_code(a_data, a_offsets, a_index, val));
                }
                for c in b_codes.iter() {
                    a_codes.push(c.map(|code| remap[*code as usize]));
                }
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
    pub(crate) fn try_upgrade_to_dict(&mut self, min_rows: usize, max_ratio: f64, max_size: usize) {
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
        let mut data: Vec<u8> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];
        let mut index: rustc_hash::FxHashMap<Box<str>, i32> = rustc_hash::FxHashMap::default();
        let mut codes = NullableColumn::with_capacity(old.len());
        for v in old.iter() {
            match v {
                Some(s) => {
                    let idx = dict_code(&mut data, &mut offsets, &mut index, s);
                    codes.push(Some(idx));
                }
                None => codes.push(None),
            }
        }
        *self = ColumnBuilder::Dictionary {
            codes,
            data,
            offsets,
            index,
        };
    }

    /// Mutable access to dictionary codes for in-place remap.
    /// Returns `None` if not a Dictionary variant.
    pub(crate) fn dict_codes_mut(&mut self) -> Option<&mut NullableColumn<i32>> {
        match self {
            ColumnBuilder::Dictionary { codes, .. } => Some(codes),
            _ => None,
        }
    }

    /// Replace the dictionary values and index (after unification).
    /// No-op if not a Dictionary variant.
    pub(crate) fn replace_dict(
        &mut self,
        new_data: Vec<u8>,
        new_offsets: Vec<i32>,
        new_index: rustc_hash::FxHashMap<Box<str>, i32>,
    ) {
        if let ColumnBuilder::Dictionary {
            data,
            offsets,
            index,
            ..
        } = self
        {
            *data = new_data;
            *offsets = new_offsets;
            *index = new_index;
        }
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
            ColumnBuilder::Decimal128(scale, _) => {
                DataType::Decimal128(38, *scale as i8)
            }
            ColumnBuilder::Dictionary { .. } => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
        }
    }

    /// Build a native Arrow array from the column builder.
    /// Build a native Arrow array from the column builder.
    /// Takes `&mut self` to move buffers instead of copying (zero-copy export).
    #[allow(
        clippy::wrong_self_convention,
        reason = "takes &mut self for zero-copy mem::take"
    )]
    pub(crate) fn to_arrow_array(&mut self) -> Result<ArrayRef> {
        // Handle empty columns (after mem::take from a previous export).
        if self.len() == 0 {
            return Ok(match self {
                ColumnBuilder::String(_) => Arc::new(StringArray::from(vec![None::<&str>; 0])),
                ColumnBuilder::Int64(_) => Arc::new(Int64Array::from(vec![None::<i64>; 0])),
                ColumnBuilder::Float64(_) => Arc::new(Float64Array::from(vec![None::<f64>; 0])),
                ColumnBuilder::Boolean(_) => Arc::new(BooleanArray::from(vec![None::<bool>; 0])),
                ColumnBuilder::Date32(_) => Arc::new(Date32Array::from(vec![None::<i32>; 0])),
                ColumnBuilder::Timestamp(unit, _) => match unit {
                    TimeUnit::Second => {
                        Arc::new(PrimitiveArray::<TimestampSecondType>::from(vec![
                            None::<i64>;
                            0
                        ]))
                    }
                    TimeUnit::Millisecond => {
                        Arc::new(PrimitiveArray::<TimestampMillisecondType>::from(vec![
                            None::<
                                i64,
                            >;
                            0
                        ]))
                    }
                    TimeUnit::Microsecond => {
                        Arc::new(PrimitiveArray::<TimestampMicrosecondType>::from(vec![
                            None::<
                                i64,
                            >;
                            0
                        ]))
                    }
                    TimeUnit::Nanosecond => {
                        Arc::new(PrimitiveArray::<TimestampNanosecondType>::from(vec![
                            None::<
                                i64,
                            >;
                            0
                        ]))
                    }
                },
                ColumnBuilder::Decimal128(scale, _) => {
                    use arrow::datatypes::Decimal128Type;
                    let arr: Decimal128Array = PrimitiveArray::from(vec![None::<i128>; 0]);
                    Arc::new(arr.with_precision_and_scale(38, *scale as i8).unwrap())
                }
                ColumnBuilder::Dictionary { .. } => {
                    let keys: Int32Array = vec![None::<i32>; 0].into_iter().collect();
                    let values: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; 0]));
                    Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values)?)
                }
            });
        }
        Ok(match self {
            ColumnBuilder::String(v) => v.to_arrow()?,
            ColumnBuilder::Int64(v) => v.to_arrow::<arrow::datatypes::Int64Type>()?,
            ColumnBuilder::Float64(v) => v.to_arrow::<arrow::datatypes::Float64Type>()?,
            ColumnBuilder::Boolean(v) => v.to_arrow_bool()?,
            ColumnBuilder::Date32(v) => v.to_arrow::<arrow::datatypes::Date32Type>()?,
            ColumnBuilder::Timestamp(unit, v) => match unit {
                TimeUnit::Second => v.to_arrow::<TimestampSecondType>()?,
                TimeUnit::Millisecond => v.to_arrow::<TimestampMillisecondType>()?,
                TimeUnit::Microsecond => v.to_arrow::<TimestampMicrosecondType>()?,
                TimeUnit::Nanosecond => v.to_arrow::<TimestampNanosecondType>()?,
            },
            ColumnBuilder::Decimal128(scale, v) => {
                use arrow::datatypes::Decimal128Type;
                let values: Vec<Option<i128>> = (0..v.len()).map(|i| v.get(i)).collect();
                let arr: Decimal128Array = values.into_iter().collect();
                Arc::new(arr.with_precision_and_scale(38, *scale as i8)?)
            },
            ColumnBuilder::Dictionary {
                codes,
                data,
                offsets,
                ..
            } => {
                use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
                let codes_arr: Int32Array = std::mem::take(codes).into_options().collect();
                // Zero-copy: wrap contiguous data+offsets directly into StringArray.
                // Dictionary values are always non-null, so validity is None.
                let offsets_buf = OffsetBuffer::new(ScalarBuffer::from(std::mem::take(offsets)));
                let data_buf = Buffer::from_vec(std::mem::take(data));
                let values: ArrayRef = Arc::new(StringArray::try_new(offsets_buf, data_buf, None)?);
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
        b.push_value(Value::Str(Cow::Borrowed("42")));
        b.push_value(Value::Str(Cow::Borrowed("bad")));
        b.push_value(Value::Null);
        if let ColumnBuilder::Int64(v) = &b {
            assert_eq!(v.get(0), Some(42));
            assert_eq!(v.get(1), None);
            assert_eq!(v.get(2), None);
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
            assert_eq!(v.get(0), Some(7));
            assert_eq!(v.get(1), Some(3));
        } else {
            panic!("expected Int64");
        }

        let mut f = ColumnBuilder::with_capacity(2, &FieldType::Float64);
        f.push_value(Value::Float64(2.5));
        f.push_value(Value::Int64(1));
        if let ColumnBuilder::Float64(v) = &f {
            assert!((v.get(0).unwrap() - 2.5).abs() < 1e-9);
            assert!((v.get(1).unwrap() - 1.0).abs() < 1e-9);
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
            assert_eq!(v.get(0), Some(7));
            assert_eq!(v.get(1), Some(3));
            assert_eq!(v.get(2), None);
            assert_eq!(v.get(3), None);
        } else {
            panic!("expected Int64 builder");
        }

        // Float64 column: native Float64, Int64 widened, Bool becomes null.
        let mut f = ColumnBuilder::with_capacity(3, &FieldType::Float64);
        f.push_value(Value::Float64(2.5));
        f.push_value(Value::Int64(1));
        f.push_value(Value::Bool(false));
        if let ColumnBuilder::Float64(v) = &f {
            assert!((v.get(0).unwrap() - 2.5).abs() < 1e-9);
            assert!((v.get(1).unwrap() - 1.0).abs() < 1e-9);
            assert_eq!(v.get(2), None);
        } else {
            panic!("expected Float64 builder");
        }

        // Boolean column: native Bool, Str parsed, Int64 becomes null.
        let mut b = ColumnBuilder::with_capacity(4, &FieldType::Boolean);
        b.push_value(Value::Bool(true));
        b.push_value(Value::Str(Cow::Borrowed("false")));
        b.push_value(Value::Str(Cow::Borrowed("not_bool")));
        b.push_value(Value::Int64(1));
        if let ColumnBuilder::Boolean(v) = &b {
            assert_eq!(v.get(0), Some(true));
            assert_eq!(v.get(1), Some(false));
            assert_eq!(v.get(2), None);
            assert_eq!(v.get(3), None);
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
        if let ColumnBuilder::Dictionary {
            codes,
            data,
            offsets,
            ..
        } = &d
        {
            // Verify dictionary values via the contiguous buffer
            let vals: Vec<&str> = (0..offsets.len() - 1)
                .map(|i| {
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    std::str::from_utf8(&data[start..end]).unwrap()
                })
                .collect();
            assert_eq!(vals, vec!["7", "true"]);
            assert_eq!(
                codes.iter().map(|o| o.copied()).collect::<Vec<_>>(),
                vec![Some(0), Some(0), Some(1), None]
            );
        } else {
            panic!("expected Dictionary builder");
        }
    }

    /// Helper: build a ValidityBitmap with `n` rows, alternating valid/invalid
    /// starting with valid. Pattern: row 0 = valid, row 1 = invalid, etc.
    fn bitmap_alternating(n: usize) -> ValidityBitmap {
        let mut b = ValidityBitmap::with_capacity(n);
        for i in 0..n {
            b.push(i % 2 == 0);
        }
        b
    }

    /// Verify `ValidityBitmap::split_off` at byte-boundary-crossing row counts.
    #[test]
    fn test_bitmap_split_off_boundaries() {
        for n in [7, 8, 9] {
            let mut bm = bitmap_alternating(n);
            let split = n / 2;
            let prefix = bm.split_off(split);
            assert_eq!(prefix.len(), split);
            assert_eq!(bm.len(), n - split);
            // Verify each bit survived the split.
            for i in 0..split {
                assert_eq!(prefix.is_valid(i), i % 2 == 0, "prefix row {i} n={n}");
            }
            for i in 0..(n - split) {
                assert_eq!(bm.is_valid(i), (i + split) % 2 == 0, "suffix row {i} n={n}");
            }
        }
    }

    /// Verify `ValidityBitmap::append` when `self` is at boundary row counts.
    #[test]
    fn test_bitmap_append_at_boundaries() {
        for n in [7, 8, 9] {
            let mut a = bitmap_alternating(n);
            let b = bitmap_alternating(5);
            a.append(&b);
            assert_eq!(a.len(), n + 5);
            for i in 0..n {
                assert_eq!(a.is_valid(i), i % 2 == 0, "original row {i} n={n}");
            }
            for i in 0..5 {
                assert_eq!(a.is_valid(n + i), i % 2 == 0, "appended row {i} n={n}");
            }
        }
    }

    /// Verify `NullableColumn<i64>::split_off` at boundary row counts.
    #[test]
    fn test_nullable_i64_split_off_boundaries() {
        for n in [7, 8, 9] {
            let mut col = NullableColumn::with_capacity(n);
            for i in 0..n {
                if i % 3 == 0 {
                    col.push(None);
                } else {
                    col.push(Some(i as i64));
                }
            }
            let split = n / 2;
            let prefix = col.split_off(split);
            assert_eq!(prefix.len(), split);
            assert_eq!(col.len(), n - split);
            for i in 0..split {
                let expected = if i % 3 == 0 { None } else { Some(i as i64) };
                assert_eq!(prefix.get(i).copied(), expected, "prefix row {i} n={n}");
            }
            for i in 0..(n - split) {
                let orig = i + split;
                let expected = if orig % 3 == 0 {
                    None
                } else {
                    Some(orig as i64)
                };
                assert_eq!(col.get(i).copied(), expected, "suffix row {i} n={n}");
            }
        }
    }

    /// Verify `StrColumn::split_off` at boundary row counts with mixed nulls.
    #[test]
    fn test_str_column_split_off_boundaries() {
        for n in [7, 8, 9] {
            let mut col = StrColumn::with_capacity(n);
            for i in 0..n {
                if i % 4 == 0 {
                    col.push(None);
                } else {
                    col.push(Some(&format!("v{i}")));
                }
            }
            let split = n / 2;
            let prefix = col.split_off(split);
            assert_eq!(prefix.len(), split);
            assert_eq!(col.len(), n - split);
            for i in 0..split {
                let expected = if i % 4 == 0 {
                    None
                } else {
                    Some(format!("v{i}"))
                };
                assert_eq!(
                    prefix.get(i).map(|s| s.to_owned()),
                    expected,
                    "prefix row {i} n={n}"
                );
            }
            for i in 0..(n - split) {
                let orig = i + split;
                let expected = if orig % 4 == 0 {
                    None
                } else {
                    Some(format!("v{orig}"))
                };
                assert_eq!(
                    col.get(i).map(|s| s.to_owned()),
                    expected,
                    "suffix row {i} n={n}"
                );
            }
        }
    }
}
