use signed_midpoint::{format_label, midpoint_floor};
#[test]
fn public_api_and_unrelated_behavior() {
    assert_eq!(midpoint_floor(-3, 0), -2);
    assert_eq!(format_label("a"), "value:a");
}
