use reservation_ledger::{Ledger, LedgerError};
use std::collections::BTreeMap;
#[test]
fn public_api() {
    let _: fn(BTreeMap<u8, u32>) -> Ledger = Ledger::new;
    let _: fn(&mut Ledger, &[(u8, u32)]) -> Result<(), LedgerError> = Ledger::reserve;
    let _: fn(&mut Ledger, &[(u8, u32)]) -> Result<(), LedgerError> = Ledger::release;
    let _: fn(&Ledger) -> Vec<(u8, u32, u32)> = Ledger::snapshot;
    let l = Ledger::new(BTreeMap::new());
    assert_eq!(l.label(), "ledger-v1");
    assert!(l.snapshot().is_empty());
}
