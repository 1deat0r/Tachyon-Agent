use crate::{Catalog, CatalogError};
impl Catalog {
    pub fn remove(&mut self, id: u8) -> Result<String, CatalogError> {
        self.entries.remove(&id).ok_or(CatalogError::UnknownId)
    }
}
