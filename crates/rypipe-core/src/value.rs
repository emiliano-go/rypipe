use std::borrow::Cow;

/// A logical value produced by a decoder.
///
/// Decoders can emit borrowed strings when possible, or native typed values
/// for formats like JSON that already distinguish numbers/booleans.
///
/// `Str` is a `Cow` so that adapters needing owned storage (e.g. XML entity
/// unescaping) can move an owned `String` into the value without a dangling
/// borrow.
#[derive(Clone, Debug, PartialEq)]
pub enum Value<'a> {
    /// UTF-8 string, borrowed from the input buffer or owned.
    Str(Cow<'a, str>),
    /// 64-bit signed integer.
    Int64(i64),
    /// 64-bit floating point number.
    Float64(f64),
    /// Boolean.
    Bool(bool),
    /// Calendar date: days since the Unix epoch (Arrow `Date32`).
    Date32(i32),
    /// Point in time as a raw integer in the column's `TimeUnit`
    /// (Arrow `Timestamp`). Adapters should declare the unit via
    /// `field_types` so the raw integer is interpreted correctly.
    Timestamp(i64),
    /// Explicit null / missing value.
    Null,
}

impl<'a> Value<'a> {
    /// Borrow the string content.
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s.as_ref()),
            _ => None,
        }
    }

    /// Convert into a `Value<'static>` by cloning any borrowed string data.
    pub fn into_static(self) -> Value<'static> {
        match self {
            Value::Str(s) => Value::Str(Cow::Owned(s.into_owned())),
            Value::Int64(i) => Value::Int64(i),
            Value::Float64(f) => Value::Float64(f),
            Value::Bool(b) => Value::Bool(b),
            Value::Date32(d) => Value::Date32(d),
            Value::Timestamp(t) => Value::Timestamp(t),
            Value::Null => Value::Null,
        }
    }
}


