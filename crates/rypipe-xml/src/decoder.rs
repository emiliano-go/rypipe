//! Crystal Reports XML decoder.

use std::borrow::Cow;

use quick_xml::events::{attributes::Attribute, Event};
use quick_xml::Reader;

use rypipe_core::{ColumnarSink, RecordParser, Value};

use crate::error::Error;
use crate::splitter::{find_special_regions, next_row_start};

/// Decoder for Crystal Reports XML row streams.
///
/// Parses `<Row ...>` elements (or a configurable row tag) into field events
/// that can be fed into any `rypipe_core::ColumnarSink`, typically a
/// [`TableBuilder`](rypipe_core::engine::TableBuilder).
#[derive(Clone, Debug, Default)]
pub struct CrystalXmlDecoder {
    row_tag: Vec<u8>,
}

impl CrystalXmlDecoder {
    /// Create a decoder that looks for `<Row>` elements.
    pub fn new() -> Self {
        Self {
            row_tag: b"Row".to_vec(),
        }
    }

    /// Create a decoder with a custom row element name.
    pub fn with_row_tag(row_tag: impl AsRef<[u8]>) -> Self {
        Self {
            row_tag: row_tag.as_ref().to_vec(),
        }
    }
}

impl RecordParser for CrystalXmlDecoder {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        self.parse_bytes_quickxml(bytes, sink)?;
        Ok(())
    }
}

impl CrystalXmlDecoder {
    fn parse_bytes_quickxml(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<(), Error> {
        // Borrowed-slice reader: events reference `bytes` directly, so no event
        // is ever copied into a scratch buffer.
        let mut reader = Reader::from_reader(bytes);
        reader.config_mut().check_end_names = false;

        let row_tag = &self.row_tag;

        loop {
            let event = match reader.read_event() {
                Ok(e) => e,
                Err(_) => {
                    let err_pos = reader.buffer_position() as usize;
                    if Self::trailing_close_tags_only(bytes, err_pos) {
                        return Ok(());
                    }
                    // Fall back to per-row scanning for malformed chunks or a
                    // partial trailing row at EOF.
                    return self.parse_tail(bytes, row_tag, err_pos, sink);
                }
            };

            match event {
                Event::Empty(ref e) if e.name().as_ref() == row_tag => {
                    sink.begin_row();
                    for attr in e.attributes() {
                        let attr = attr.map_err(|e| {
                            Error::XmlParse(reader.buffer_position() as usize, e.to_string())
                        })?;
                        let key = utf8_unchecked(attr.key.as_ref());
                        let value = attr_value(&attr)?;
                        sink.put_field(key, Value::Str(value.as_ref()));
                    }
                    sink.end_row();
                }

                Event::Start(ref e) if e.name().as_ref() == row_tag => {
                    sink.begin_row();
                    for attr in e.attributes() {
                        let attr = attr.map_err(|e| {
                            Error::XmlParse(reader.buffer_position() as usize, e.to_string())
                        })?;
                        let key = utf8_unchecked(attr.key.as_ref());
                        let value = attr_value(&attr)?;
                        sink.put_field(key, Value::Str(value.as_ref()));
                    }

                    loop {
                        let child_event = match reader.read_event() {
                            Ok(e) => e,
                            Err(_) => {
                                let err_pos = reader.buffer_position() as usize;
                                return self.parse_tail(bytes, row_tag, err_pos, sink);
                            }
                        };

                        match child_event {
                            Event::Start(ref child) | Event::Empty(ref child) => {
                                let child_name = child.name();
                                let child_tag = child_name.as_ref();

                                if child_tag == b"Field" {
                                    let mut field_name = None;
                                    for attr in child.attributes().flatten() {
                                        let attr_key = attr.key.as_ref();
                                        if attr_key == b"FieldName" || attr_key == b"Name" {
                                            if let Ok(value) = attr_value(&attr) {
                                                field_name = Some(value);
                                                break;
                                            }
                                        }
                                    }
                                    let key = field_name.as_deref().unwrap_or("Field");

                                    let mut text: Cow<'_, str> = Cow::Borrowed("");
                                    if matches!(child_event, Event::Start(_)) {
                                        let field_end_bytes: &[u8] = b"Field";
                                        loop {
                                            let inner = match reader.read_event() {
                                                Ok(e) => e,
                                                Err(_) => {
                                                    let err_pos = reader.buffer_position() as usize;
                                                    return self
                                                        .parse_tail(bytes, row_tag, err_pos, sink);
                                                }
                                            };
                                            match inner {
                                                Event::Start(ref inner_child)
                                                | Event::Empty(ref inner_child) => {
                                                    let inner_name = inner_child.name();
                                                    let inner_tag = inner_name.as_ref();
                                                    if (inner_tag == b"FormattedValue"
                                                        || inner_tag == b"Value")
                                                        && matches!(inner, Event::Start(_))
                                                    {
                                                        let text_event = match reader.read_event() {
                                                            Ok(e) => e,
                                                            Err(_) => {
                                                                let err_pos = reader
                                                                    .buffer_position()
                                                                    as usize;
                                                                return self.parse_tail(
                                                                    bytes, row_tag, err_pos, sink,
                                                                );
                                                            }
                                                        };
                                                        if let Event::Text(txt) = text_event {
                                                            text = text_value(txt)?;
                                                        }
                                                    }
                                                }
                                                Event::End(ref e)
                                                    if e.name().as_ref() == field_end_bytes =>
                                                {
                                                    break;
                                                }
                                                Event::Eof => return Ok(()),
                                                _ => {}
                                            }
                                        }
                                    }
                                    sink.put_field(key, Value::Str(text.as_ref()));
                                } else if child_tag == b"Text" {
                                    let mut text_name = None;
                                    for attr in child.attributes().flatten() {
                                        if attr.key.as_ref() == b"Name" {
                                            if let Ok(value) = attr_value(&attr) {
                                                text_name = Some(value);
                                                break;
                                            }
                                        }
                                    }
                                    let key = text_name.as_deref().unwrap_or("Text");

                                    let mut text: Cow<'_, str> = Cow::Borrowed("");
                                    if matches!(child_event, Event::Start(_)) {
                                        let text_end_bytes: &[u8] = b"Text";
                                        loop {
                                            let inner = match reader.read_event() {
                                                Ok(e) => e,
                                                Err(_) => {
                                                    let err_pos = reader.buffer_position() as usize;
                                                    return self
                                                        .parse_tail(bytes, row_tag, err_pos, sink);
                                                }
                                            };
                                            match inner {
                                                Event::Start(ref inner_child)
                                                | Event::Empty(ref inner_child) => {
                                                    let ic_name = inner_child.name();
                                                    let ic_tag = ic_name.as_ref();
                                                    if ic_tag == b"TextValue"
                                                        && matches!(inner, Event::Start(_))
                                                    {
                                                        let text_event = match reader.read_event() {
                                                            Ok(e) => e,
                                                            Err(_) => {
                                                                let err_pos = reader
                                                                    .buffer_position()
                                                                    as usize;
                                                                return self.parse_tail(
                                                                    bytes, row_tag, err_pos, sink,
                                                                );
                                                            }
                                                        };
                                                        if let Event::Text(txt) = text_event {
                                                            text = text_value(txt)?;
                                                        }
                                                    }
                                                }
                                                Event::End(ref e)
                                                    if e.name().as_ref() == text_end_bytes =>
                                                {
                                                    break;
                                                }
                                                Event::Eof => return Ok(()),
                                                _ => {}
                                            }
                                        }
                                    }
                                    sink.put_field(key, Value::Str(text.as_ref()));
                                } else if child_tag == b"Section" {
                                    let sn = child
                                        .attributes()
                                        .filter_map(|a| a.ok())
                                        .find(|a| a.key.as_ref() == b"SectionNumber")
                                        .and_then(|a| attr_value(&a).ok())
                                        .unwrap_or_default();
                                    sink.put_field("Section", Value::Str(sn.as_ref()));
                                } else {
                                    let key = utf8_unchecked(child_tag);
                                    sink.put_field(key, Value::Str(""));
                                }
                            }

                            Event::End(ref e) if e.name().as_ref() == row_tag => break,
                            Event::Eof => return Ok(()),
                            _ => {}
                        }
                    }

                    sink.end_row();
                }

                Event::Eof => return Ok(()),
                _ => {}
            }
        }
    }

    /// Fallback recovery for chunked parsing: resume by scanning for the next
    /// row start and parsing rows individually. This handles orphan parent
    /// close-tags that are valid in the full document but not within a chunk.
    fn parse_tail(
        &self,
        bytes: &[u8],
        row_tag: &[u8],
        start_pos: usize,
        sink: &mut dyn ColumnarSink,
    ) -> Result<(), Error> {
        let (skip_regions, _) = find_special_regions(bytes);
        let mut pos = start_pos;
        let row_tag_owned = row_tag.to_vec();

        while let Some(row_start) = next_row_start(bytes, pos, row_tag, &skip_regions) {
            let row_bytes = &bytes[row_start..];
            let mut rr = Reader::from_reader(row_bytes);
            rr.config_mut().check_end_names = false;

            let ev = match rr.read_event() {
                Ok(e) => e,
                Err(_) => break,
            };

            let mut row_complete = true;
            match ev {
                Event::Empty(ref e) if e.name().as_ref() == row_tag_owned => {
                    sink.begin_row();
                    for a in e.attributes().flatten() {
                        let key = std::str::from_utf8(a.key.as_ref()).unwrap_or("");
                        let value = a.unescape_value().unwrap_or_default();
                        sink.put_field(key, Value::Str(value.as_ref()));
                    }
                    sink.end_row();
                }
                Event::Start(ref e) if e.name().as_ref() == row_tag_owned => {
                    sink.begin_row();
                    for a in e.attributes().flatten() {
                        let key = std::str::from_utf8(a.key.as_ref()).unwrap_or("");
                        let value = a.unescape_value().unwrap_or_default();
                        sink.put_field(key, Value::Str(value.as_ref()));
                    }

                    loop {
                        let child_event = match rr.read_event() {
                            Ok(e) => e,
                            Err(_) => {
                                row_complete = false;
                                break;
                            }
                        };
                        match child_event {
                            Event::Start(ref child) | Event::Empty(ref child) => {
                                let child_name = child.name();
                                let tag = child_name.as_ref();
                                if tag == b"Field" {
                                    let mut name = String::from("Field");
                                    for a in child.attributes().flatten() {
                                        let k = a.key.as_ref();
                                        if k == b"FieldName" || k == b"Name" {
                                            if let Ok(v) = a.unescape_value() {
                                                name = v.into_owned();
                                                break;
                                            }
                                        }
                                    }
                                    let mut text = String::new();
                                    if matches!(child_event, Event::Start(_)) {
                                        let field_end: &[u8] = b"Field";
                                        loop {
                                            let inner = rr.read_event();
                                            match inner {
                                                Ok(Event::Start(ref ic)) => {
                                                    let ic_name = ic.name();
                                                    let ic_tag = ic_name.as_ref();
                                                    if ic_tag == b"FormattedValue"
                                                        || ic_tag == b"Value"
                                                    {
                                                        if let Ok(Event::Text(txt)) =
                                                            rr.read_event()
                                                        {
                                                            if let Ok(v) = txt.unescape() {
                                                                text = v.into_owned();
                                                            }
                                                        }
                                                    }
                                                }
                                                Ok(Event::Empty(ref ic)) => {
                                                    let ic_name = ic.name();
                                                    let ic_tag = ic_name.as_ref();
                                                    if ic_tag == b"FormattedValue"
                                                        || ic_tag == b"Value"
                                                    {
                                                        // Empty inner element leaves text empty.
                                                    }
                                                }
                                                Ok(Event::End(ref ne))
                                                    if ne.name().as_ref() == field_end =>
                                                {
                                                    break;
                                                }
                                                Ok(Event::Eof) => {
                                                    row_complete = false;
                                                    break;
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                    sink.put_field(&name, Value::Str(&text));
                                } else if tag == b"Text" {
                                    let mut name = String::from("Text");
                                    for a in child.attributes().flatten() {
                                        if a.key.as_ref() == b"Name" {
                                            if let Ok(v) = a.unescape_value() {
                                                name = v.into_owned();
                                                break;
                                            }
                                        }
                                    }
                                    let mut text = String::new();
                                    if matches!(child_event, Event::Start(_)) {
                                        let text_end: &[u8] = b"Text";
                                        loop {
                                            let inner = rr.read_event();
                                            match inner {
                                                Ok(Event::Start(ref ic))
                                                | Ok(Event::Empty(ref ic)) => {
                                                    let ic_name = ic.name();
                                                    let ic_tag = ic_name.as_ref();
                                                    if ic_tag == b"TextValue" {
                                                        if let Ok(Event::Text(txt)) =
                                                            rr.read_event()
                                                        {
                                                            if let Ok(v) = txt.unescape() {
                                                                text = v.into_owned();
                                                            }
                                                        }
                                                    }
                                                }
                                                Ok(Event::End(ref ne))
                                                    if ne.name().as_ref() == text_end =>
                                                {
                                                    break;
                                                }
                                                Ok(Event::Eof) => {
                                                    row_complete = false;
                                                    break;
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                    sink.put_field(&name, Value::Str(&text));
                                } else if tag == b"Section" {
                                    let sn = child
                                        .attributes()
                                        .filter_map(|a| a.ok())
                                        .find(|a| a.key.as_ref() == b"SectionNumber")
                                        .and_then(|a| a.unescape_value().ok())
                                        .unwrap_or_default()
                                        .into_owned();
                                    sink.put_field("Section", Value::Str(&sn));
                                } else {
                                    let key = std::str::from_utf8(tag).unwrap_or("");
                                    sink.put_field(key, Value::Str(""));
                                }
                            }
                            Event::End(ref e) if e.name().as_ref() == row_tag_owned => break,
                            Event::Eof => {
                                row_complete = false;
                                break;
                            }
                            _ => {}
                        }
                    }
                    if row_complete {
                        sink.end_row();
                    }
                }
                _ => {}
            }
            if !row_complete {
                break;
            }
            pos = row_start + 1;
        }
        Ok(())
    }

    fn trailing_close_tags_only(bytes: &[u8], mut pos: usize) -> bool {
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }

        if pos >= bytes.len() {
            return true;
        }

        while pos < bytes.len() {
            if bytes[pos] != b'<' || pos + 1 >= bytes.len() || bytes[pos + 1] != b'/' {
                return false;
            }
            pos += 2;

            while pos < bytes.len() && bytes[pos] != b'>' {
                pos += 1;
            }
            if pos >= bytes.len() {
                return false;
            }
            pos += 1;

            while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
        }

        true
    }
}

/// Bytes are chunk-validated UTF-8 (see `validate`); skip std's per-call revalidation.
#[allow(unsafe_code)]
#[inline]
fn utf8_unchecked(b: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(b) }
}

/// Attribute value without revalidation; unescapes only when an entity is
/// actually present (memchr probe: CR values almost never contain `&`).
fn attr_value<'v>(attr: &Attribute<'v>) -> Result<Cow<'v, str>, Error> {
    match &attr.value {
        Cow::Borrowed(b) => {
            let s = utf8_unchecked(b);
            if memchr::memchr(b'&', b).is_none() {
                Ok(Cow::Borrowed(s))
            } else {
                quick_xml::escape::unescape(s).map_err(|e| Error::XmlParse(0, e.to_string()))
            }
        }
        // Owned never occurs for the borrowed-slice reader; fall back.
        Cow::Owned(_) => attr
            .unescape_value()
            .map_err(|e| Error::XmlParse(0, e.to_string()))
            .map(|c| Cow::Owned(c.into_owned())),
    }
}

/// Text content without revalidation; same `&` probe as `attr_value`.
fn text_value(txt: quick_xml::events::BytesText<'_>) -> Result<Cow<'_, str>, Error> {
    match txt.into_inner() {
        Cow::Borrowed(b) => {
            let s = utf8_unchecked(b);
            if memchr::memchr(b'&', b).is_none() {
                Ok(Cow::Borrowed(s))
            } else {
                quick_xml::escape::unescape(s).map_err(|e| Error::XmlParse(0, e.to_string()))
            }
        }
        Cow::Owned(o) => {
            let s = String::from_utf8(o).map_err(|e| Error::XmlParse(0, e.to_string()))?;
            Ok(
                match quick_xml::escape::unescape(&s)
                    .map_err(|e| Error::XmlParse(0, e.to_string()))?
                {
                    Cow::Borrowed(x) => Cow::Owned(x.to_owned()),
                    Cow::Owned(x) => Cow::Owned(x),
                },
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use rypipe_core::engine::TableBuilder;

    fn parse(xml: &[u8]) -> TableBuilder {
        let mut sink = TableBuilder::with_capacity(4);
        CrystalXmlDecoder::new()
            .parse_chunk(xml, &mut sink)
            .unwrap();
        sink
    }

    #[test]
    fn test_row_attributes() {
        let xml = br#"<Rows><Row A="1" B="hello"/></Rows>"#;
        let mut sink = parse(xml);
        assert_eq!(sink.num_rows(), 1);
        let batch = sink.finish().unwrap();
        let a = batch.column_by_name("A").unwrap().as_string::<i32>();
        let b = batch.column_by_name("B").unwrap().as_string::<i32>();
        assert_eq!(a.value(0), "1");
        assert_eq!(b.value(0), "hello");
    }

    #[test]
    fn test_field_child() {
        let xml = br#"<Row><Field Name="X"><Value>42</Value></Field></Row>"#;
        let mut sink = parse(xml);
        assert_eq!(sink.num_rows(), 1);
        let batch = sink.finish().unwrap();
        let x = batch.column_by_name("X").unwrap().as_string::<i32>();
        assert_eq!(x.value(0), "42");
    }

    #[test]
    fn test_text_child() {
        let xml = br#"<Row><Text Name="Title"><TextValue>Report</TextValue></Text></Row>"#;
        let mut sink = parse(xml);
        assert_eq!(sink.num_rows(), 1);
        let batch = sink.finish().unwrap();
        let title = batch.column_by_name("Title").unwrap().as_string::<i32>();
        assert_eq!(title.value(0), "Report");
    }

    #[test]
    fn test_section_child() {
        let xml = br#"<Row><Section SectionNumber="3"/></Row>"#;
        let mut sink = parse(xml);
        assert_eq!(sink.num_rows(), 1);
        let batch = sink.finish().unwrap();
        let section = batch.column_by_name("Section").unwrap().as_string::<i32>();
        assert_eq!(section.value(0), "3");
    }

    #[test]
    fn test_unknown_child() {
        let xml = br#"<Row><Custom/></Row>"#;
        let mut sink = parse(xml);
        assert_eq!(sink.num_rows(), 1);
        let batch = sink.finish().unwrap();
        let custom = batch.column_by_name("Custom").unwrap().as_string::<i32>();
        assert_eq!(custom.value(0), "");
    }

    #[test]
    fn test_empty_input() {
        let sink = parse(b"");
        assert_eq!(sink.num_rows(), 0);
    }

    #[test]
    fn test_partial_trailing_row_discarded() {
        let xml = br#"<Row><Field Name="X"><Value>1</Value></Field></Row><Row><Field Name="X""#;
        let mut sink = parse(xml);
        assert_eq!(sink.num_rows(), 1);
        let batch = sink.finish().unwrap();
        let x = batch.column_by_name("X").unwrap().as_string::<i32>();
        assert_eq!(x.value(0), "1");
    }
}
