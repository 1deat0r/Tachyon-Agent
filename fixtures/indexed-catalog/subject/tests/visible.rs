use indexed_catalog::{Catalog, CatalogError};
#[test]
fn visible_insert_rejects_owned_name() {
    let mut c = Catalog::new();
    c.insert(0, "a").unwrap();
    let before = c.snapshot();
    assert_eq!(c.insert(1, "a"), Err(CatalogError::DuplicateName));
    assert_eq!(c.snapshot(), before);
}
#[test]
fn visible_rename_preserves_state_on_collision() {
    let mut c = Catalog::new();
    c.insert(0, "a").unwrap();
    c.insert(1, "b").unwrap();
    let before = c.snapshot();
    assert_eq!(c.rename(0, "b"), Err(CatalogError::DuplicateName));
    assert_eq!(c.snapshot(), before);
}
#[test]
fn visible_remove_clears_reverse_index() {
    let mut c = Catalog::new();
    c.insert(0, "a").unwrap();
    assert_eq!(c.remove(0), Ok("a".to_string()));
    assert_eq!(c.name(0), None);
    assert_eq!(c.owner("a"), None);
}
