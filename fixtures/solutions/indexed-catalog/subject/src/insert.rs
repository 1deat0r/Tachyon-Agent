use crate::{Catalog, CatalogError};
impl Catalog {
    pub fn insert(&mut self, id: u8, name: &str) -> Result<(), CatalogError> {
        if self.entries.contains_key(&id) {
            return Err(CatalogError::DuplicateId);
        }
        if self.owners.contains_key(name) {
            return Err(CatalogError::DuplicateName);
        }
        self.entries.insert(id, name.to_string());
        self.owners.insert(name.to_string(), id);
        Ok(())
    }
}
