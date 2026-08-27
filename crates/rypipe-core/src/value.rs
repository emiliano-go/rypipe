/// A logical value produced by a decoder.
///
/// Decoders can emit borrowed strings when possible, or native typed values
/// for formats like JSON that already distinguish numbers/booleans.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value<'a> {
    /// UTF-8 string borrowed from the input buffer.
    Str(&'a str),
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
