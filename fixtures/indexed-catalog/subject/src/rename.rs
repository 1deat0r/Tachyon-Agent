use crate::{Catalog, CatalogError};
impl Catalog {
    pub fn rename(&mut self, id: u8, name: &str) -> Result<(), CatalogError> {
        let old = self.entries.get_mut(&id).ok_or(CatalogError::UnknownId)?;
        let previous = old.clone();
        *old = name.to_string();
        if self.owners.get(name).is_some_and(|owner| *owner != id) {
            return Err(CatalogError::DuplicateName);
        }
        self.owners.remove(&previous);
        self.owners.insert(name.to_string(), id);
        Ok(())
    }
}
