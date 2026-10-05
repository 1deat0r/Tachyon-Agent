use ceiling_division::ceil_div;
#[test]
fn boundaries_match_wide_arithmetic() {
    for n in [0, 1, 2, 3, 9, u64::MAX - 1, u64::MAX] {
        for d in [0, 1, 2, 3, 10, u64::MAX - 1, u64::MAX] {
            let expected = if d == 0 {
                None
            } else {
                Some(((n as u128 + d as u128 - 1) / d as u128) as u64)
            };
            assert_eq!(ceil_div(n, d), expected);
        }
    }
}
