use atomic_ledger::{Ledger, TransferError};
#[test]
fn visible_overflow_is_atomic() {
    let mut l = Ledger::new(7, u64::MAX, "fixture");
    assert_eq!(l.transfer(2), Err(TransferError::Overflow));
    assert_eq!(l.balances(), (7, u64::MAX));
}
