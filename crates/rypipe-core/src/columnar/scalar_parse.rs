use crate::plan::FieldType;
use arrow::datatypes::TimeUnit;

/// Whether `s` parses into the declared type `ft`, mirroring the lenient
/// conversions in [`super::ColumnBuilder::push_str`]. String and dictionary columns
/// accept any string. Used by strict-types mode to detect malformed values.
pub(crate) fn parses_as(s: &str, ft: &FieldType) -> bool {
    match ft {
        FieldType::String | FieldType::Dictionary => true,
        FieldType::Int64 => lexical::parse::<i64, _>(s.as_bytes()).is_ok(),
        FieldType::Float64 => lexical::parse::<f64, _>(s.as_bytes()).is_ok(),
        FieldType::Boolean => s.parse::<bool>().is_ok(),
        FieldType::Date32 => parse_date32(s).is_some(),
        FieldType::Timestamp(unit, fmt) => parse_timestamp(s, *unit, fmt.as_deref()).is_some(),
        FieldType::Decimal128(scale) => parse_decimal128(s, *scale).is_some(),
    }
}

/// Parse an ISO-8601 date (`YYYY-MM-DD`) into days since the Unix epoch.
pub fn parse_date32(s: &str) -> Option<i32> {
    let d = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some((d - epoch).num_days() as i32)
}

/// Parse a decimal string into a scaled `i128` for Arrow `Decimal128`.
/// Handles signs and fractional parts; extra fraction digits beyond `scale`
/// are truncated. Returns `None` for non-numeric input.
pub fn parse_decimal128(s: &str, scale: u8) -> Option<i128> {
    let s = s.trim();
    let (neg, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = s.split_once('.').map_or((s, ""), |(i, f)| (i, f));
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part
        .bytes()
        .chain(frac_part.bytes())
        .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let int_val: i128 = if int_part.is_empty() {
        0
    } else {
        int_part.parse().ok()?
    };
    let scale = scale as u32;
    let mut val = int_val.checked_mul(10i128.checked_pow(scale)?)?;
    if !frac_part.is_empty() {
        let digits = &frac_part[..frac_part.len().min(scale as usize)];
        if !digits.is_empty() {
            let frac: i128 = digits.parse().ok()?;
            let frac_scaled = frac.checked_mul(10i128.checked_pow(scale - digits.len() as u32)?)?;
            val = val.checked_add(frac_scaled)?;
        }
    }
    Some(if neg { -val } else { val })
}

pub(crate) fn format_decimal128(value: i128, scale: u8) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let sign = if value.is_negative() { "-" } else { "" };
    let mut digits = value.unsigned_abs().to_string();
    let scale = scale as usize;
    if digits.len() <= scale {
        digits = format!("{:0>width$}", digits, width = scale + 1);
    }
    let split = digits.len() - scale;
    format!("{sign}{}.{}", &digits[..split], &digits[split..])
}

/// Parse an ISO-8601 datetime (or bare date = midnight) into an integer in
/// `unit`. Naive parsing only; adapters handling timezones should emit
/// `Value::Timestamp` directly.
pub fn parse_timestamp(s: &str, unit: TimeUnit, format: Option<&str>) -> Option<i64> {
    let t = s.trim();
    let dt = format
        .and_then(|fmt| {
            chrono::NaiveDateTime::parse_from_str(t, fmt)
                .ok()
                .or_else(|| {
                    chrono::NaiveDate::parse_from_str(t, fmt)
                        .ok()
                        .map(|d| d.and_hms_opt(0, 0, 0).expect("midnight is valid"))
                })
        })
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| {
                    chrono::NaiveDate::parse_from_str(t, "%Y-%m-%d")
                        .map(|d| d.and_hms_opt(0, 0, 0).expect("midnight is valid"))
                })
                .ok()
        })?;
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
pub(crate) fn format_date32(days: i32) -> String {
    match chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days as i64)))
    {
        Some(d) => d.format("%Y-%m-%d").to_string(),
        None => days.to_string(),
    }
}

/// Format a raw timestamp integer to ISO-8601 (`YYYY-MM-DDTHH:MM:SS(.fff…)`).
pub(crate) fn format_timestamp(v: i64, unit: TimeUnit) -> String {
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
