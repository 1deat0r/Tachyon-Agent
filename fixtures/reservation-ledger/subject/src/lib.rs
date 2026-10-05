use std::collections::BTreeMap;
mod release;
mod reserve;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerError {
    UnknownSku(u8),
    QuantityOverflow(u8),
    Insufficient(u8),
    NotHeld(u8),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ledger {
    stock: BTreeMap<u8, u32>,
    held: BTreeMap<u8, u32>,
}
impl Ledger {
    pub fn new(stock: BTreeMap<u8, u32>) -> Self {
        let held = stock.keys().map(|k| (*k, 0)).collect();
        Self { stock, held }
    }
    pub fn snapshot(&self) -> Vec<(u8, u32, u32)> {
        self.stock
            .iter()
            .map(|(k, v)| (*k, *v, self.held[k]))
            .collect()
    }
    pub fn label(&self) -> &'static str {
        "ledger-v1"
    }
}
