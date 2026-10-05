use utf8_text::TextLabel;
#[test]
fn public_api_and_label_are_preserved() {
    let mut t = TextLabel::new("abcd", "stable");
    t.truncate_bytes(2);
    let _: &str = t.text();
    let _: &str = t.label();
    assert_eq!(t.text(), "ab");
    assert_eq!(t.label(), "stable");
}
