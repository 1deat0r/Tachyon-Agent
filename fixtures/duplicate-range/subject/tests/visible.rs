use sorted_range::equal_range;
#[test]
fn visible_duplicates_include_every_match() {
    assert_eq!(equal_range(&[1, 2, 2, 2, 4], 2), 1..4);
}
