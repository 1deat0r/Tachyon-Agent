use utf8_text::TextLabel;
#[test]
fn all_byte_budgets_are_maximal_prefixes() {
    for original in ["", "ascii", "éø", "a🦀z", "e\u{301}中", "🦀🦀"] {
        for budget in 0..=original.len() + 3 {
            let mut t = TextLabel::new(original, "keep-label");
            t.truncate_bytes(budget);
            let expected: String = original
                .chars()
                .scan(0, |bytes, c| {
                    *bytes += c.len_utf8();
                    Some((*bytes, c))
                })
                .take_while(|(bytes, _)| *bytes <= budget)
                .map(|(_, c)| c)
                .collect();
            assert_eq!(t.text(), expected);
            assert_eq!(t.label(), "keep-label");
            t.truncate_bytes(budget);
            assert_eq!(t.text(), expected);
        }
    }
}
