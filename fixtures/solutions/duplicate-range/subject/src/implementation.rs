use std::ops::Range;
pub fn equal_range(values: &[i64], needle: i64) -> Range<usize> {
    let start = values.partition_point(|v| *v < needle);
    let end = values.partition_point(|v| *v <= needle);
    start..end
}
pub fn is_ordered(values: &[i64]) -> bool {
    values.windows(2).all(|w| w[0] <= w[1])
}
pub fn format_label(name: &str) -> String {
    format!("index:{name}")
}
