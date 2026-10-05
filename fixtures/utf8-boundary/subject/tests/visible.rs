use utf8_text::TextLabel;
#[test]
fn visible_utf8_boundary() {
    let mut t = TextLabel::new("aéz", "fixture");
    t.truncate_bytes(2);
    assert_eq!(t.text(), "a");
    assert_eq!(t.label(), "fixture");
}
