#![no_main]
use libfuzzer_sys::fuzz_target;
use rypipe_core::decoder::{find_next_record_boundary, is_continued_newline};

fuzz_target!(|input: (Vec<u8>, usize, Option<u8>, bool)| {
    let (bytes, from, continuation, blank) = input;
    for from in [from, from % (bytes.len() + 1)] {
        let _ = is_continued_newline(&bytes, from);
        if let Some(end) =
            find_next_record_boundary(&bytes, from, continuation, &[b"#", b"!"], blank)
        {
            assert!(end > from && end <= bytes.len());
            assert_eq!(bytes[end - 1], b'\n');
            if blank {
                assert!(bytes[..end].ends_with(b"\n\n") || bytes[..end].ends_with(b"\n\r\n"));
            }
        }
    }
});
