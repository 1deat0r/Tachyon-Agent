use crate::{Ledger, LedgerError};
impl Ledger {
    pub fn release(&mut self, batch: &[(u8, u32)]) -> Result<(), LedgerError> {
        for (sku, quantity) in batch {
            let available = self
                .held
                .get_mut(sku)
                .ok_or(LedgerError::UnknownSku(*sku))?;
            if *available < *quantity {
                return Err(LedgerError::NotHeld(*sku));
            }
            *available -= *quantity;
            *self.stock.get_mut(sku).unwrap() += *quantity;
        }
        Ok(())
    }
}
