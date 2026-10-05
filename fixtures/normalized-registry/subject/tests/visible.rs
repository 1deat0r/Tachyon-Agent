use normalized_registry::{RegisterError, Registry};
#[test]
fn visible_writer_duplicate() {
    let mut r = Registry::new("tag");
    r.register("API", 1).unwrap();
    assert_eq!(r.register("api", 2), Err(RegisterError::Duplicate));
    assert_eq!(r.len(), 1);
}
#[test]
fn visible_reader_normalizes() {
    let mut r = Registry::new("tag");
    r.register("api", 1).unwrap();
    assert_eq!(r.lookup(" API "), Some(1));
}
