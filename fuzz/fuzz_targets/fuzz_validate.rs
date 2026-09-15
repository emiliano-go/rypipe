#![no_main]
use libfuzzer_sys::fuzz_target;
use rypipe_core::RecordParser;
use rypipe_fuzz::{ini, ldif, properties};

fuzz_target!(|bytes: &[u8]| {
    let valid = std::str::from_utf8(bytes).is_ok();
    let parsers: [&dyn RecordParser; 3] = [
        &properties::PropertiesParser,
        &ini::IniParser,
        &ldif::LdifParser,
    ];
    for parser in parsers {
        assert_eq!(parser.validate(bytes).is_ok(), valid);
    }
});
