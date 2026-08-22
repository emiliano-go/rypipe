use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Int32Type};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use crate::plan::FieldType;
use crate::value::Value;
use crate::Result;

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

    fn len(&self) -> usize {
        self.validity.len()
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

    fn to_arrow(&self) -> Result<ArrayRef> {
        use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
        let offsets = OffsetBuffer::new(ScalarBuffer::from(self.offsets.clone()));
        let data = Buffer::from_slice_ref(&self.data);
        let nulls = if self.validity.iter().all(|&v| v) {
            None
        } else {
            Some(NullBuffer::from(self.validity.clone()))
        };
        let arr = StringArray::try_new(offsets, data, nulls)?;
        Ok(Arc::new(arr))
    }
}

/// Per-column builder: stores all values.  The variant determines the storage
/// type (String, Int64, Float64, Boolean, or Dictionary).
pub(crate) enum ColumnBuilder {
    String(StrColumn),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Boolean(Vec<Option<bool>>),
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

impl ColumnBuilder {
    pub(crate) fn with_capacity(cap: usize, field_type: &FieldType) -> Self {
        match field_type {
            FieldType::String => ColumnBuilder::String(StrColumn::with_capacity(cap)),
            FieldType::Int64 => ColumnBuilder::Int64(Vec::with_capacity(cap)),
            FieldType::Float64 => ColumnBuilder::Float64(Vec::with_capacity(cap)),
            FieldType::Boolean => ColumnBuilder::Boolean(Vec::with_capacity(cap)),
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
        }
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
            ColumnBuilder::Dictionary { codes, .. } => drop(codes.pop()),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            ColumnBuilder::String(v) => v.len(),
            ColumnBuilder::Int64(v) => v.len(),
            ColumnBuilder::Float64(v) => v.len(),
            ColumnBuilder::Boolean(v) => v.len(),
            ColumnBuilder::Dictionary { codes, .. } => codes.len(),
        }
    }

    /// Value at `index` formatted as a string for filter comparison.
    pub(crate) fn get_filter_value(&self, index: usize) -> Option<String> {
        match self {
            ColumnBuilder::String(v) => v.get(index).map(|s| s.to_owned()),
            ColumnBuilder::Int64(v) => v.get(index).and_then(|o| o.map(|n| n.to_string())),
            ColumnBuilder::Float64(v) => v.get(index).and_then(|o| o.map(|n| n.to_string())),
            ColumnBuilder::Boolean(v) => v.get(index).and_then(|o| o.map(|n| n.to_string())),
            ColumnBuilder::Dictionary { codes, dict, .. } => codes
                .get(index)
                .and_then(|code| code.map(|idx| dict[idx as usize].clone())),
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
            _ => return Err(crate::Error::Merge(
                "extend_owned: column type mismatch across chunks".to_string(),
            )),
        }
        Ok(())
    }

    /// Upgrade a String builder to Dictionary if cardinality is low enough.
    /// No-op if not String, or if rows < min_rows.
    pub(crate) fn try_upgrade_to_dict(&mut self, min_rows: usize) {
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
        // Threshold: at most 5% distinct, clamped to [16, 256].
        let threshold = (old.len() / 20).clamp(16, 256);
        if seen.len() > threshold {
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
            ColumnBuilder::Dictionary { .. } => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
        }
    }

    /// Build a native Arrow array from the column builder.
    pub(crate) fn to_arrow_array(&self) -> Result<ArrayRef> {
        Ok(match self {
            ColumnBuilder::String(v) => v.to_arrow()?,
            ColumnBuilder::Int64(v) => Arc::new(v.iter().copied().collect::<Int64Array>()),
            ColumnBuilder::Float64(v) => Arc::new(v.iter().copied().collect::<Float64Array>()),
            ColumnBuilder::Boolean(v) => Arc::new(v.iter().copied().collect::<BooleanArray>()),
            ColumnBuilder::Dictionary { codes, dict, .. } => {
                let keys: Int32Array = codes.iter().copied().collect();
                let values: ArrayRef =
                    Arc::new(dict.iter().map(|s| Some(s.as_str())).collect::<StringArray>());
                let arr = DictionaryArray::<Int32Type>::try_new(keys, values)?;
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
        assert_eq!(b.as_str_vec(), vec![Some("42".into()), Some("true".into()), None]);
    }

    #[test]
    fn test_all_value_variants_by_column_type() {
        // Int64 column: native Int64, Float64 widened, Bool becomes null.
        let mut i = ColumnBuilder::with_capacity(4, &FieldType::Int64);
        i.push_value(Value::Int64(7));
        i.push_value(Value::Float64(3.9)); // truncated to 3
        i.push_value(Value::Bool(true));   // unsupported -> null
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
