use atomic_ledger::{Ledger, TransferError};
#[test]
fn rejected_and_successful_transfers_preserve_state_contract() {
    for source in [0, 1, 3, u64::MAX] {
        for destination in [0, 1, u64::MAX - 1, u64::MAX] {
            for amount in [0, 1, 2, u64::MAX] {
                let mut l = Ledger::new(source, destination, "stable");
                let result = l.transfer(amount);
                if amount > source {
                    assert_eq!(result, Err(TransferError::InsufficientFunds));
                    assert_eq!(l.balances(), (source, destination));
                } else if u128::from(destination) + u128::from(amount) > u128::from(u64::MAX) {
                    assert_eq!(result, Err(TransferError::Overflow));
                    assert_eq!(l.balances(), (source, destination));
                } else {
                    assert_eq!(result, Ok(()));
                    assert_eq!(l.balances(), (source - amount, destination + amount));
                }
                assert_eq!(l.label(), "stable");
            }
        }
    }
}
