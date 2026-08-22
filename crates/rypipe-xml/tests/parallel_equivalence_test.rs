//! Multi-chunk equivalence: the same XML parsed in one pass and via the
//! parallel executor must produce identical `RecordBatch` output.

use arrow::array::{Array, AsArray};
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use rypipe_core::engine::TableBuilder;
use rypipe_core::parallel::ParallelExecutor;
use rypipe_core::{ExecutionPlan, RecordParser};
use rypipe_xml::{CrystalXmlDecoder, CrystalXmlSplitter};

fn parse_single(xml: &[u8]) -> RecordBatch {
    let mut sink = TableBuilder::with_capacity(4);
    CrystalXmlDecoder::new()
        .parse_chunk(xml, &mut sink)
        .unwrap();
    sink.finish().unwrap()
}

fn parse_parallel(xml: &[u8], num_chunks: usize) -> RecordBatch {
    let batches = ParallelExecutor::parse(
        xml,
        &CrystalXmlSplitter::new(),
        CrystalXmlDecoder::new(),
        ExecutionPlan::new(),
        num_chunks,
    )
    .unwrap();

    assert!(
        !batches.is_empty(),
        "parallel parse of a non-empty file must return at least one batch"
    );
    assert!(
        batches.iter().any(|b| b.num_rows() > 0),
        "at least one batch must be non-empty"
    );

    let schema = batches[0].schema();
    concat_batches(&schema, batches.iter().collect::<Vec<_>>()).unwrap()
}

#[test]
fn test_parallel_equivalence_full_document() {
    let xml = br#"<CrystalReport>
        <Details>
            <Row A="1"><Field Name="X"><Value>10</Value></Field></Row>
            <Row A="2"><Field Name="X"><Value>20</Value></Field></Row>
            <Row A="3"><Field Name="X"><Value>30</Value></Field></Row>
            <Row A="4"><Field Name="X"><Value>40</Value></Field></Row>
            <Row A="5"><Field Name="X"><Value>50</Value></Field></Row>
            <Row A="6"><Field Name="X"><Value>60</Value></Field></Row>
            <Row A="7"><Field Name="X"><Value>70</Value></Field></Row>
            <Row A="8"><Field Name="X"><Value>80</Value></Field></Row>
        </Details>
    </CrystalReport>"#;

    let single = parse_single(xml);
    let parallel = parse_parallel(xml, 4);

    assert_eq!(single.num_rows(), parallel.num_rows());
    assert_eq!(single.schema(), parallel.schema());
    assert_eq!(single.num_columns(), parallel.num_columns());

    for name in ["A", "X"] {
        let expected = single.column_by_name(name).unwrap();
        let actual = parallel.column_by_name(name).unwrap();
        assert_eq!(actual.data_type(), &DataType::Utf8);
        let expected_arr = expected.as_string::<i32>();
        let actual_arr = actual.as_string::<i32>();
        assert_eq!(actual_arr.len(), expected_arr.len());
        for i in 0..expected_arr.len() {
            assert_eq!(actual_arr.is_null(i), expected_arr.is_null(i));
            if !expected_arr.is_null(i) {
                assert_eq!(actual_arr.value(i), expected_arr.value(i));
            }
        }
    }
}
