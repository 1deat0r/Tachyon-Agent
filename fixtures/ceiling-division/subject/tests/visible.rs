use ceiling_division::ceil_div;
#[test]
fn visible_regression() {
    assert_eq!(ceil_div(u64::MAX, 2), Some(1u64 << 63));
}
