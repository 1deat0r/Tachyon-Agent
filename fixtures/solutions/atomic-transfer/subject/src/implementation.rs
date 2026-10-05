#[derive(Debug, PartialEq, Eq)]
pub enum TransferError {
    InsufficientFunds,
    Overflow,
}
#[derive(Debug)]
pub struct Ledger {
    source: u64,
    destination: u64,
    label: String,
}
impl Ledger {
    pub fn new(source: u64, destination: u64, label: impl Into<String>) -> Self {
        Self {
            source,
            destination,
            label: label.into(),
        }
    }
    pub fn transfer(&mut self, amount: u64) -> Result<(), TransferError> {
        let source = self
            .source
            .checked_sub(amount)
            .ok_or(TransferError::InsufficientFunds)?;
        let destination = self
            .destination
            .checked_add(amount)
            .ok_or(TransferError::Overflow)?;
        self.source = source;
        self.destination = destination;
        Ok(())
    }
    pub fn balances(&self) -> (u64, u64) {
        (self.source, self.destination)
    }
    pub fn label(&self) -> &str {
        &self.label
    }
}
