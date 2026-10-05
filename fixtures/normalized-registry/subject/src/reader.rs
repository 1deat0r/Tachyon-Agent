use crate::Registry;
impl Registry {
    pub fn lookup(&self, name: &str) -> Option<u64> {
        self.entries.get(name).copied()
    }
}
