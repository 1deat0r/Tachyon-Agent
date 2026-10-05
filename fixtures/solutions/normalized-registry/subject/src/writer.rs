use crate::{RegisterError, Registry};
impl Registry {
    pub fn register(&mut self, name: &str, value: u64) -> Result<(), RegisterError> {
        let key = name.trim().to_ascii_lowercase();
        if key.is_empty() {
            return Err(RegisterError::EmptyName);
        }
        if self.entries.contains_key(&key) {
            return Err(RegisterError::Duplicate);
        }
        self.entries.insert(key, value);
        Ok(())
    }
}
