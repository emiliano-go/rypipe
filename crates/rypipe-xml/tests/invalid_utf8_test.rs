//! Error-path tests for the Crystal XML decoder.

use rypipe_core::RecordParser;
use rypipe_xml::CrystalXmlDecoder;

#[test]
fn test_validate_rejects_invalid_utf8() {
    let invalid = vec![0xff, 0xfe, 0xfd];
    let result = CrystalXmlDecoder::new().validate(&invalid);
    assert!(result.is_err(), "invalid UTF-8 must be rejected");
}
