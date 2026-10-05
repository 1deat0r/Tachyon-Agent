use normalized_registry::{RegisterError, Registry};
#[test]
fn public_api_is_preserved() {
    let _: fn(&mut Registry, &str, u64) -> Result<(), RegisterError> = Registry::register;
    let _: fn(&Registry, &str) -> Option<u64> = Registry::lookup;
    let r = Registry::new("tag");
    assert_eq!(r.len(), 0);
    assert!(r.is_empty());
    assert_eq!(r.label(), "tag");
}
