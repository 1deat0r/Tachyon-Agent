use crate::{Ledger, LedgerError};
use std::collections::BTreeMap;
impl Ledger {
    pub fn release(&mut self, batch: &[(u8, u32)]) -> Result<(), LedgerError> {
        let mut totals = BTreeMap::<u8, u64>::new();
        for (sku, quantity) in batch {
            *totals.entry(*sku).or_insert(0) += u64::from(*quantity);
        }
        for sku in totals.keys() {
            if !self.stock.contains_key(sku) {
                return Err(LedgerError::UnknownSku(*sku));
            }
        }
        for (sku, quantity) in &totals {
            if *quantity > u64::from(u32::MAX) {
                return Err(LedgerError::QuantityOverflow(*sku));
            }
        }
        for (sku, quantity) in &totals {
            if u64::from(self.held[sku]) < *quantity {
                return Err(LedgerError::NotHeld(*sku));
            }
        }
        for (sku, quantity) in totals {
            let quantity = quantity as u32;
            *self.held.get_mut(&sku).unwrap() -= quantity;
            *self.stock.get_mut(&sku).unwrap() += quantity;
        }
        Ok(())
    }
}
