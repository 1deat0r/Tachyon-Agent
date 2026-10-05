use signed_midpoint::midpoint_floor;
#[test]
fn boundaries_match_wide_arithmetic() {
    for a in [
        i64::MIN,
        i64::MIN + 1,
        -5,
        -1,
        0,
        1,
        4,
        i64::MAX - 1,
        i64::MAX,
    ] {
        for b in [
            i64::MIN,
            i64::MIN + 1,
            -5,
            -1,
            0,
            1,
            4,
            i64::MAX - 1,
            i64::MAX,
        ] {
            assert_eq!(
                midpoint_floor(a, b),
                ((a as i128 + b as i128).div_euclid(2)) as i64
            );
        }
    }
}
