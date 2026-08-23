//! `rypipe-xml`: Crystal Reports XML adapter for `rypipe-core`.
//!
//! Provides a [`CrystalXmlDecoder`] that emits field events for Crystal Reports
//! XML rows and a [`CrystalXmlSplitter`] that finds row-boundary split points
//! for parallel/bounded parsing.

pub mod decoder;
pub mod error;
pub mod splitter;

pub use decoder::CrystalXmlDecoder;
pub use splitter::CrystalXmlSplitter;

use rypipe_core::Pipeline;

/// Create a ready-to-run pipeline for Crystal Reports XML with a custom row
/// tag.
///
/// ```no_run
/// use rypipe_xml::xml_pipeline;
///
/// let pipeline = xml_pipeline("Row");
/// let batch = pipeline.read_path("report.xml", false, false).unwrap();
/// ```
pub fn xml_pipeline(
    row_tag: impl AsRef<[u8]>,
) -> Pipeline<CrystalXmlSplitter, CrystalXmlDecoder> {
    let tag = row_tag.as_ref().to_vec();
    Pipeline::new(
        CrystalXmlSplitter::with_row_tag(&tag),
        CrystalXmlDecoder::with_row_tag(&tag),
    )
}

/// Create a ready-to-run pipeline for Crystal Reports XML using the default
/// `Row` tag.
pub fn default_pipeline() -> Pipeline<CrystalXmlSplitter, CrystalXmlDecoder> {
    xml_pipeline(b"Row")
}
