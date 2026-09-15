#![no_main]
use libfuzzer_sys::fuzz_target;
use rypipe_core::decoder::{find_next_record_boundary, Splitter};
use rypipe_fuzz::{ini, ldif, properties};

struct Declarative;
impl Splitter for Declarative {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        find_next_record_boundary(bytes, from, Some(b'\\'), &[b"#", b"!"], false)
    }
    fn estimate_bytes_per_row(&self, _: &[u8]) -> usize {
        1
    }
}

fuzz_target!(|input: (Vec<u8>, u8, usize)| {
    let (bytes, count, from) = input;
    let splitters: [&dyn Splitter; 4] = [
        &Declarative,
        &properties::PropertiesSplitter,
        &ini::IniSplitter,
        &ldif::LdifSplitter,
    ];
    for splitter in splitters {
        let _ = splitter.next_record_start(&bytes, from);
        let points = splitter.find_split_points(&bytes, count as usize);
        assert_eq!(points.first(), Some(&0));
        assert_eq!(points.last(), Some(&bytes.len()));
        assert!(points.iter().all(|&p| p <= bytes.len()));
        assert!(points.windows(2).all(|p| p[0] < p[1] || bytes.is_empty()));
    }
});
