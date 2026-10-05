use ceiling_division::{ceil_div, format_label};
#[test]
fn public_api_and_unrelated_behavior() {
    assert_eq!(ceil_div(9, 4), Some(3));
    assert_eq!(format_label("a"), "value:a");
}
