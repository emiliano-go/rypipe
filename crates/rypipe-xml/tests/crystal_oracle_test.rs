//! Oracle-style parity tests for Crystal XML parsing.
//!
//! Each test parses a small Crystal XML snippet with `rypipe` and compares the
//! resulting `RecordBatch` against a hand-built expected Arrow batch.

use std::sync::Arc;

use arrow::array::{Array, AsArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use rypipe_core::engine::TableBuilder;
use rypipe_core::RecordParser;
use rypipe_xml::CrystalXmlDecoder;

fn parse(xml: &[u8]) -> RecordBatch {
    let mut sink = TableBuilder::with_capacity(4);
    CrystalXmlDecoder::new()
        .parse_chunk(xml, &mut sink)
        .unwrap();
    sink.finish().unwrap()
}

fn assert_string_column(batch: &RecordBatch, name: &str, expected: &[Option<&str>]) {
    let col = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name:?}"));
    assert_eq!(
        col.data_type(),
        &DataType::Utf8,
        "column {name:?} should be UTF-8"
    );
    let arr = col.as_string::<i32>();
    assert_eq!(arr.len(), expected.len(), "column {name:?} length mismatch");
    for (i, exp) in expected.iter().enumerate() {
        assert_eq!(
            arr.is_null(i),
            exp.is_none(),
            "column {name:?} null mismatch at row {i}"
        );
        if let Some(v) = exp {
            assert_eq!(arr.value(i), *v, "column {name:?} value mismatch at row {i}");
        }
    }
}

fn column_to_str_options<'a>(batch: &'a RecordBatch, name: &str) -> Vec<Option<&'a str>> {
    let col = batch.column_by_name(name).unwrap();
    let arr = col.as_string::<i32>();
    (0..arr.len())
        .map(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) })
        .collect()
}

#[test]
fn test_crystal_xml_oracle_full_row() {
    let xml = br#"<CrystalReport>
        <Details>
            <Row A="1" B="foo">
                <Field FieldName="Y"><FormattedValue>200.5</FormattedValue></Field>
                <Field Name="X"><Value>100</Value></Field>
                <Text Name="Title"><TextValue>Hello</TextValue></Text>
                <Section SectionNumber="1"/>
                <Unknown/>
            </Row>
            <Row A="2" B="bar">
                <Field Name="X"><Value>300</Value></Field>
            </Row>
            <Row A="3" B="baz"/>
        </Details>
    </CrystalReport>"#;

    let batch = parse(xml);
    assert_eq!(batch.num_rows(), 3, "expected three rows");

    // Expected column order follows first-appearance order in the XML.
    let expected_order = vec!["A", "B", "Y", "X", "Title", "Section", "Unknown"];
    let schema = batch.schema();
    let actual_order: Vec<&str> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(actual_order, expected_order, "column order mismatch");

    // Row attributes.
    assert_string_column(&batch, "A", &[Some("1"), Some("2"), Some("3")]);
    assert_string_column(&batch, "B", &[Some("foo"), Some("bar"), Some("baz")]);

    // <Field> with FieldName / Name.
    assert_string_column(&batch, "Y", &[Some("200.5"), None, None]);
    assert_string_column(&batch, "X", &[Some("100"), Some("300"), None]);

    // <Text> with Name.
    assert_string_column(&batch, "Title", &[Some("Hello"), None, None]);

    // <Section>.
    assert_string_column(&batch, "Section", &[Some("1"), None, None]);

    // Unknown child tags are emitted as empty strings.
    assert_string_column(&batch, "Unknown", &[Some(""), None, None]);

    // Build the expected batch by hand and assert structural equality.
    let schema = Arc::new(Schema::new(
        expected_order
            .into_iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let expected = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("1"), Some("2"), Some("3")])),
            Arc::new(StringArray::from(vec![Some("foo"), Some("bar"), Some("baz")])),
            Arc::new(StringArray::from(vec![Some("200.5"), None, None])),
            Arc::new(StringArray::from(vec![Some("100"), Some("300"), None])),
            Arc::new(StringArray::from(vec![Some("Hello"), None, None])),
            Arc::new(StringArray::from(vec![Some("1"), None, None])),
            Arc::new(StringArray::from(vec![Some(""), None, None])),
        ],
    )
    .unwrap();

    assert_eq!(batch.schema(), expected.schema());
    assert_eq!(batch.num_columns(), expected.num_columns());
    for name in ["A", "B", "Y", "X", "Title", "Section", "Unknown"] {
        assert_string_column(&expected, name, &column_to_str_options(&batch, name));
    }
}
