use crate::{Catalog, CatalogError};
impl Catalog {
    pub fn remove(&mut self, id: u8) -> Result<String, CatalogError> {
        let name = self
            .entries
            .get(&id)
            .ok_or(CatalogError::UnknownId)?
            .clone();
        self.entries.remove(&id);
        self.owners.remove(&name);
        Ok(name)
    }
}
