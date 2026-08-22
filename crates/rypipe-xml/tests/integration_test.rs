use arrow::array::{Array, AsArray};
use arrow::datatypes::{Float64Type, Int32Type, Int64Type};

use rypipe_core::{engine::TableBuilder, plan::ExecutionPlan, FieldType, RecordParser, Splitter};
use rypipe_xml::{CrystalXmlDecoder, CrystalXmlSplitter};

fn parse(xml: &[u8]) -> TableBuilder {
    let mut sink = TableBuilder::with_capacity(4);
    CrystalXmlDecoder::new()
        .parse_chunk(xml, &mut sink)
        .unwrap();
    sink
}

fn parse_with_plan(xml: &[u8], plan: ExecutionPlan) -> TableBuilder {
    let mut sink = TableBuilder::with_plan(4, plan);
    CrystalXmlDecoder::new()
        .parse_chunk(xml, &mut sink)
        .unwrap();
    sink
}

#[test]
fn test_crystal_xml_full_document() {
    let xml = br#"<CrystalReport>
        <Details>
            <Row A="1" B="foo">
                <Field Name="X"><Value>100</Value></Field>
                <Field FieldName="Y"><FormattedValue>200.5</FormattedValue></Field>
                <Text Name="Title"><TextValue>Hello</TextValue></Text>
                <Section SectionNumber="1"/>
                <Unknown/>
            </Row>
            <Row A="2" B="bar">
                <Field Name="X"><Value>300</Value></Field>
            </Row>
        </Details>
    </CrystalReport>"#;

    let mut sink = parse(xml);
    assert_eq!(sink.num_rows(), 2);
    let batch = sink.finish().unwrap();

    let a = batch.column_by_name("A").unwrap().as_string::<i32>();
    assert_eq!(a.value(0), "1");
    assert_eq!(a.value(1), "2");

    let b = batch.column_by_name("B").unwrap().as_string::<i32>();
    assert_eq!(b.value(0), "foo");
    assert_eq!(b.value(1), "bar");

    let x = batch.column_by_name("X").unwrap().as_string::<i32>();
    assert_eq!(x.value(0), "100");
    assert_eq!(x.value(1), "300");

    let y = batch.column_by_name("Y").unwrap().as_string::<i32>();
    assert_eq!(y.value(0), "200.5");
    assert!(y.is_null(1));

    let title = batch.column_by_name("Title").unwrap().as_string::<i32>();
    assert_eq!(title.value(0), "Hello");
    assert!(title.is_null(1));

    let section = batch.column_by_name("Section").unwrap().as_string::<i32>();
    assert_eq!(section.value(0), "1");
    assert!(section.is_null(1));

    let unknown = batch.column_by_name("Unknown").unwrap().as_string::<i32>();
    assert_eq!(unknown.value(0), "");
    assert!(unknown.is_null(1));
}

#[test]
fn test_rename_drop_pushdown() {
    let xml = br#"<Row A="1" B="2"/><Row A="3" B="4"/>"#;
    let mut plan = ExecutionPlan::new();
    plan.field_map.insert("A".into(), "Alpha".into());
    plan.drop_fields.insert("B".into());

    let mut sink = parse_with_plan(xml, plan);
    assert_eq!(sink.num_rows(), 2);
    let batch = sink.finish().unwrap();
    assert!(batch.column_by_name("Alpha").is_some());
    assert!(batch.column_by_name("A").is_none());
    assert!(batch.column_by_name("B").is_none());

    let alpha = batch.column_by_name("Alpha").unwrap().as_string::<i32>();
    assert_eq!(alpha.value(0), "1");
    assert_eq!(alpha.value(1), "3");
}

#[test]
fn test_filter_pushdown_equal() {
    let xml = br#"<Row A="yes"/><Row A="no"/><Row A="yes"/>"#;
    let mut plan = ExecutionPlan::new();
    plan.filter = Some(rypipe_core::FilterPredicate::Equal {
        field: "A".into(),
        value: "yes".into(),
    });

    let mut sink = parse_with_plan(xml, plan);
    assert_eq!(sink.num_rows(), 2);
    let batch = sink.finish().unwrap();
    let a = batch.column_by_name("A").unwrap().as_string::<i32>();
    assert_eq!(a.value(0), "yes");
    assert_eq!(a.value(1), "yes");
}

#[test]
fn test_typed_int64_column() {
    let xml = br#"<Row><Field Name="N"><Value>10</Value></Field></Row>
                 <Row><Field Name="N"><Value>bad</Value></Field></Row>
                 <Row><Field Name="N"><Value>30</Value></Field></Row>"#;
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("N".into(), FieldType::Int64);

    let mut sink = parse_with_plan(xml, plan);
    assert_eq!(sink.num_rows(), 3);
    let batch = sink.finish().unwrap();
    let n = batch
        .column_by_name("N")
        .unwrap()
        .as_primitive::<Int64Type>();
    assert_eq!(n.value(0), 10);
    assert!(n.is_null(1));
    assert_eq!(n.value(2), 30);
}

#[test]
fn test_typed_float64_column() {
    let xml = br#"<Row><Field Name="N"><Value>1.5</Value></Field></Row>"#;
    let mut plan = ExecutionPlan::new();
    plan.field_types.insert("N".into(), FieldType::Float64);

    let mut sink = parse_with_plan(xml, plan);
    let batch = sink.finish().unwrap();
    let n = batch
        .column_by_name("N")
        .unwrap()
        .as_primitive::<Float64Type>();
    assert!((n.value(0) - 1.5).abs() < 1e-9);
}

#[test]
fn test_dictionary_column() {
    let xml = br#"<Row><Field Name="P"><Value>Widget</Value></Field></Row>
                 <Row><Field Name="P"><Value>Gadget</Value></Field></Row>
                 <Row><Field Name="P"><Value>Widget</Value></Field></Row>"#;
    let mut plan = ExecutionPlan::new();
    plan.dictionary_columns.insert("P".into());

    let mut sink = parse_with_plan(xml, plan);
    assert_eq!(sink.num_rows(), 3);
    let batch = sink.finish().unwrap();
    let dict = batch
        .column_by_name("P")
        .unwrap()
        .as_dictionary::<Int32Type>();
    let values = dict.values().as_string::<i32>();
    assert_eq!(values.value(0), "Widget");
    assert_eq!(values.value(1), "Gadget");
    assert_eq!(dict.keys().value(0), 0);
    assert_eq!(dict.keys().value(1), 1);
    assert_eq!(dict.keys().value(2), 0);
}

#[test]
fn test_splitter_and_decoder_multi_chunk_match() {
    let xml = br#"<Row><Field Name="X"><Value>1</Value></Field></Row>
                 <Row><Field Name="X"><Value>2</Value></Field></Row>
                 <Row><Field Name="X"><Value>3</Value></Field></Row>
                 <Row><Field Name="X"><Value>4</Value></Field></Row>"#;

    let single_sink = parse(xml);
    assert_eq!(single_sink.num_rows(), 4);

    let splitter = CrystalXmlSplitter::new();
    let points = splitter.find_split_points(xml, 2);
    assert!(points.len() >= 2);

    let mut merged = TableBuilder::new();
    for w in points.windows(2) {
        let chunk = &xml[w[0]..w[1]];
        let mut sink = TableBuilder::with_capacity(2);
        CrystalXmlDecoder::new()
            .parse_chunk(chunk, &mut sink)
            .unwrap();
        merged.extend(sink).unwrap();
    }
    assert_eq!(merged.num_rows(), single_sink.num_rows());
}

#[test]
fn test_validate_rejects_invalid_utf8() {
    let invalid = vec![0xff, 0xfe, 0xfd];
    let result = CrystalXmlDecoder::new().validate(&invalid);
    assert!(result.is_err());
}
