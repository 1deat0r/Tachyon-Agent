use indexed_catalog::{Catalog, CatalogError};
#[test]
fn public_api() {
    let _: fn() -> Catalog = Catalog::new;
    let _: fn(&mut Catalog, u8, &str) -> Result<(), CatalogError> = Catalog::insert;
    let _: fn(&mut Catalog, u8, &str) -> Result<(), CatalogError> = Catalog::rename;
    let _: fn(&mut Catalog, u8) -> Result<String, CatalogError> = Catalog::remove;
    let _: fn(&Catalog, u8) -> Option<&str> = Catalog::name;
    let _: fn(&Catalog, &str) -> Option<u8> = Catalog::owner;
    let _: fn(&Catalog) -> (Vec<(u8, String)>, Vec<(String, u8)>) = Catalog::snapshot;
    assert_eq!(Catalog::default(), Catalog::new());
    assert_eq!(Catalog::new().label(), "catalog-v1");
}
