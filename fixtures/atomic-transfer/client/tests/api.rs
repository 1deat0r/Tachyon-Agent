use atomic_ledger::{Ledger, TransferError};
#[test]
fn public_api_and_label_are_preserved() {
    let mut l = Ledger::new(8, 1, "stable");
    let _: Result<(), TransferError> = l.transfer(2);
    let _: (u64, u64) = l.balances();
    let _: &str = l.label();
    assert_eq!(l.balances(), (6, 3));
    assert_eq!(l.label(), "stable");
}
