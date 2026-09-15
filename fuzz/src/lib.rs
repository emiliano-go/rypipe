// Compile the actual parsers without building adapter cdylibs; cargo-fuzz's
// Windows /include:main linker flag cannot link those shared libraries.
#[path = "../../rypipe-ini/src/lib.rs"]
pub mod ini;
#[path = "../../rypipe-ldif/src/lib.rs"]
pub mod ldif;
#[path = "../../rypipe-properties/src/lib.rs"]
pub mod properties;
