use crate::{Catalog, CatalogError};
impl Catalog {
    pub fn insert(&mut self, id: u8, name: &str) -> Result<(), CatalogError> {
        if self.entries.contains_key(&id) {
            return Err(CatalogError::DuplicateId);
        }
        self.entries.insert(id, name.to_string());
        self.owners.insert(name.to_string(), id);
        Ok(())
    }
}
