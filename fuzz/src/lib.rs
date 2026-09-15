// Compile the actual parsers without building adapter cdylibs; cargo-fuzz's
// Windows /include:main linker flag cannot link those shared libraries.
#[path = "../../examples/rypipe-ini/src/lib.rs"]
pub mod ini;
#[path = "../../examples/rypipe-ldif/src/lib.rs"]
pub mod ldif;
#[path = "../../examples/rypipe-properties/src/lib.rs"]
pub mod properties;
