#![no_main]
use libfuzzer_sys::fuzz_target;
use rypipe_core::{RecordParser, TableBuilder};
use rypipe_fuzz::{ini, ldif, properties};

fuzz_target!(|bytes: &[u8]| {
    let parsers: [&dyn RecordParser; 3] = [
        &properties::PropertiesParser,
        &ini::IniParser,
        &ldif::LdifParser,
    ];
    for parser in parsers {
        let mut sink = TableBuilder::new();
        if parser.parse_chunk(bytes, &mut sink).is_ok() {
            let batch = sink
                .finish()
                .expect("successful parse must export valid Arrow");
            assert!(batch
                .columns()
                .iter()
                .all(|column| column.len() == batch.num_rows()));
        }
    }
});
