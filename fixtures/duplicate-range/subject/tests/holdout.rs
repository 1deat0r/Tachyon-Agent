use sorted_range::equal_range;
#[test]
fn duplicate_and_absent_boundaries_match_linear_oracle() {
    for values in [
        vec![],
        vec![2],
        vec![2, 2, 2],
        vec![i64::MIN, 0, 0, i64::MAX],
        vec![-3, -3, -1, 0, 0, 0, 4, 4],
    ] {
        for needle in [i64::MIN, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, i64::MAX] {
            let start = values.iter().filter(|v| **v < needle).count();
            let count = values.iter().filter(|v| **v == needle).count();
            assert_eq!(equal_range(&values, needle), start..start + count);
        }
    }
}
