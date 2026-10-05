use reservation_ledger::{Ledger, LedgerError};
#[test]
fn visible_reserve_is_atomic() {
    let mut l = Ledger::new([(0, 3), (1, 1)].into());
    let before = l.snapshot();
    assert_eq!(
        l.reserve(&[(0, 1), (1, 2)]),
        Err(LedgerError::Insufficient(1))
    );
    assert_eq!(l.snapshot(), before);
}
#[test]
fn visible_release_is_atomic() {
    let mut l = Ledger::new([(0, 3), (1, 3)].into());
    l.reserve(&[(0, 2), (1, 1)]).unwrap();
    let before = l.snapshot();
    assert_eq!(l.release(&[(0, 1), (1, 2)]), Err(LedgerError::NotHeld(1)));
    assert_eq!(l.snapshot(), before);
}
