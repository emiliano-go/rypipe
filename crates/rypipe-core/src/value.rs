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
    /// Explicit null / missing value.
    Null,
}
