use std::collections::BTreeMap;
mod insert;
mod remove;
mod rename;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    DuplicateId,
    DuplicateName,
    UnknownId,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    entries: BTreeMap<u8, String>,
    owners: BTreeMap<String, u8>,
}
impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
impl Catalog {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            owners: BTreeMap::new(),
        }
    }
    pub fn name(&self, id: u8) -> Option<&str> {
        self.entries.get(&id).map(String::as_str)
    }
    pub fn owner(&self, name: &str) -> Option<u8> {
        self.owners.get(name).copied()
    }
    pub fn snapshot(&self) -> (Vec<(u8, String)>, Vec<(String, u8)>) {
        (
            self.entries.iter().map(|(k, v)| (*k, v.clone())).collect(),
            self.owners.iter().map(|(k, v)| (k.clone(), *v)).collect(),
        )
    }
    pub fn label(&self) -> &'static str {
        "catalog-v1"
    }
}
