use crate::{Ledger, LedgerError};
impl Ledger {
    pub fn reserve(&mut self, batch: &[(u8, u32)]) -> Result<(), LedgerError> {
        for (sku, quantity) in batch {
            let available = self
                .stock
                .get_mut(sku)
                .ok_or(LedgerError::UnknownSku(*sku))?;
            if *available < *quantity {
                return Err(LedgerError::Insufficient(*sku));
            }
            *available -= *quantity;
            *self.held.get_mut(sku).unwrap() += *quantity;
        }
        Ok(())
    }
}
