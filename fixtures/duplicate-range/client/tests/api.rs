use sorted_range::{equal_range, format_label, is_ordered};
#[test]
fn public_api_and_other_functions_are_preserved() {
    let _: fn(&[i64], i64) -> std::ops::Range<usize> = equal_range;
    assert!(is_ordered(&[1, 1, 2]));
    assert!(!is_ordered(&[2, 1]));
    assert_eq!(format_label("stable"), "index:stable");
}
