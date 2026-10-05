use signed_midpoint::midpoint_floor;
#[test]
fn visible_regression() {
    assert_eq!(midpoint_floor(i64::MAX, i64::MAX), i64::MAX);
}
