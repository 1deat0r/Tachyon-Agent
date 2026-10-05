mod reader;
mod writer;
use std::collections::BTreeMap;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterError {
    EmptyName,
    Duplicate,
}
pub struct Registry {
    entries: BTreeMap<String, u64>,
    label: String,
}
impl Registry {
    pub fn new(label: &str) -> Self {
        Self {
            entries: BTreeMap::new(),
            label: label.to_owned(),
        }
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn label(&self) -> &str {
        &self.label
    }
}
