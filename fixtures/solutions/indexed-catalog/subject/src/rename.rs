use crate::{Catalog, CatalogError};
impl Catalog {
    pub fn rename(&mut self, id: u8, name: &str) -> Result<(), CatalogError> {
        let previous = self
            .entries
            .get(&id)
            .ok_or(CatalogError::UnknownId)?
            .clone();
        if self.owners.get(name).is_some_and(|owner| *owner != id) {
            return Err(CatalogError::DuplicateName);
        }
        self.owners.remove(&previous);
        self.entries.insert(id, name.to_string());
        self.owners.insert(name.to_string(), id);
        Ok(())
    }
}
