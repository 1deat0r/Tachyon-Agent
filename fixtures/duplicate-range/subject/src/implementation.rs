use std::ops::Range;
pub fn equal_range(values: &[i64], needle: i64) -> Range<usize> {
    match values.binary_search(&needle) {
        Ok(i) => i..i + 1,
        Err(i) => i..i,
    }
}
pub fn is_ordered(values: &[i64]) -> bool {
    values.windows(2).all(|w| w[0] <= w[1])
}
pub fn format_label(name: &str) -> String {
    format!("index:{name}")
}
