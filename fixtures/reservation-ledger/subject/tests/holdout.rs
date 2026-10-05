use reservation_ledger::{Ledger, LedgerError};
// Independent vector state, linear SKU scans and wider totals; no fixture helpers.
fn reference(
    state: &mut [(u8, u32, u32)],
    batch: &[(u8, u32)],
    release: bool,
) -> Result<(), LedgerError> {
    let mut unknown: Vec<_> = batch
        .iter()
        .filter(|(k, _)| !state.iter().any(|(id, _, _)| id == k))
        .map(|(k, _)| *k)
        .collect();
    unknown.sort();
    if let Some(k) = unknown.first() {
        return Err(LedgerError::UnknownSku(*k));
    }
    let sums: Vec<_> = state
        .iter()
        .map(|(id, _, _)| {
            (
                *id,
                batch
                    .iter()
                    .filter(|(k, _)| k == id)
                    .map(|(_, n)| u64::from(*n))
                    .sum::<u64>(),
            )
        })
        .collect();
    for (id, n) in &sums {
        if *n > u64::from(u32::MAX) {
            return Err(LedgerError::QuantityOverflow(*id));
        }
    }
    for ((id, stock, held), (_, n)) in state.iter().zip(&sums) {
        if *n > u64::from(if release { *held } else { *stock }) {
            return Err(if release {
                LedgerError::NotHeld(*id)
            } else {
                LedgerError::Insufficient(*id)
            });
        }
    }
    for ((_, stock, held), (_, n)) in state.iter_mut().zip(sums) {
        let n = n as u32;
        if release {
            *held -= n;
            *stock += n;
        } else {
            *stock -= n;
            *held += n;
        }
    }
    Ok(())
}
#[test]
fn sequences_match_independent_state_oracle() {
    let ops: Vec<(bool, Vec<(u8, u32)>)> = vec![
        (false, vec![]),
        (false, vec![(0, 1)]),
        (false, vec![(0, 1), (0, 1)]),
        (false, vec![(0, 1), (1, 2)]),
        (false, vec![(0, 1), (2, 0)]),
        (true, vec![]),
        (true, vec![(0, 1)]),
        (true, vec![(0, 1), (0, 1)]),
        (true, vec![(0, 1), (1, 2)]),
    ];
    for (a, b) in [(0, 0), (1, 2), (3, 1)] {
        for i in 0..ops.len() {
            for j in 0..ops.len() {
                for k in 0..ops.len() {
                    let mut l = Ledger::new([(0, a), (1, b)].into());
                    let mut expected = vec![(0, a, 0), (1, b, 0)];
                    for index in [i, j, k] {
                        let (release, batch) = &ops[index];
                        let before = expected.clone();
                        let wanted = reference(&mut expected, batch, *release);
                        let got = if *release {
                            l.release(batch)
                        } else {
                            l.reserve(batch)
                        };
                        assert_eq!(
                            got, wanted,
                            "initial={a},{b} sequence={i},{j},{k} index={index}"
                        );
                        assert_eq!(l.snapshot(), expected);
                        if wanted.is_err() {
                            assert_eq!(expected, before);
                        }
                        assert_eq!(l.label(), "ledger-v1");
                    }
                }
            }
        }
    }
}
#[test]
fn integer_limits_and_error_precedence_preserve_state() {
    let mut l = Ledger::new([(0, u32::MAX), (1, 2)].into());
    let before = l.snapshot();
    assert_eq!(
        l.reserve(&[(0, u32::MAX), (0, 1)]),
        Err(LedgerError::QuantityOverflow(0))
    );
    assert_eq!(l.snapshot(), before);
    assert_eq!(
        l.reserve(&[(0, u32::MAX), (0, 1), (2, 0)]),
        Err(LedgerError::UnknownSku(2))
    );
    assert_eq!(l.snapshot(), before);
    l.reserve(&[(0, u32::MAX), (1, 2)]).unwrap();
    let full = l.snapshot();
    assert_eq!(
        l.release(&[(0, u32::MAX), (0, 1)]),
        Err(LedgerError::QuantityOverflow(0))
    );
    assert_eq!(l.snapshot(), full);
    assert_eq!(l.release(&[(2, 0)]), Err(LedgerError::UnknownSku(2)));
    assert_eq!(l.snapshot(), full);
    l.release(&[(0, u32::MAX), (1, 2)]).unwrap();
    assert_eq!(l.snapshot(), before);
    assert_eq!(
        l.reserve(&[(1, 3), (0, u32::MAX), (0, 1)]),
        Err(LedgerError::QuantityOverflow(0))
    );
    assert_eq!(l.snapshot(), before);
    assert_eq!(
        l.reserve(&[(3, 0), (2, 0)]),
        Err(LedgerError::UnknownSku(2))
    );
    assert_eq!(l.snapshot(), before);
}

#[test]
fn smallest_sku_precedence_does_not_follow_batch_order() {
    let mut ledger = Ledger::new([(0, 0), (1, 0)].into());
    let before = ledger.snapshot();
    assert_eq!(
        ledger.reserve(&[(1, 1), (0, 1)]),
        Err(LedgerError::Insufficient(0))
    );
    assert_eq!(ledger.snapshot(), before);
    assert_eq!(
        ledger.release(&[(1, 1), (0, 1)]),
        Err(LedgerError::NotHeld(0))
    );
    assert_eq!(ledger.snapshot(), before);
    let overflow = [(1, u32::MAX), (1, 1), (0, u32::MAX), (0, 1)];
    assert_eq!(
        ledger.reserve(&overflow),
        Err(LedgerError::QuantityOverflow(0))
    );
    assert_eq!(ledger.snapshot(), before);
    assert_eq!(
        ledger.release(&overflow),
        Err(LedgerError::QuantityOverflow(0))
    );
    assert_eq!(ledger.snapshot(), before);
    assert_eq!(
        ledger.reserve(&[(1, u32::MAX), (1, 1), (3, 0), (2, 0)]),
        Err(LedgerError::UnknownSku(2))
    );
    assert_eq!(ledger.snapshot(), before);
    assert_eq!(
        ledger.release(&[(1, u32::MAX), (1, 1), (3, 0), (2, 0)]),
        Err(LedgerError::UnknownSku(2))
    );
    assert_eq!(ledger.snapshot(), before);
}
