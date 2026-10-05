use crate::{RegisterError, Registry};
impl Registry {
    pub fn register(&mut self, name: &str, value: u64) -> Result<(), RegisterError> {
        if name.is_empty() {
            return Err(RegisterError::EmptyName);
        }
        if self.entries.contains_key(name) {
            return Err(RegisterError::Duplicate);
        }
        self.entries.insert(name.to_owned(), value);
        Ok(())
    }
}
