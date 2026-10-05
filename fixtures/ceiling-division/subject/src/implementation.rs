pub fn ceil_div(numerator: u64, denominator: u64) -> Option<u64> {
    if denominator == 0 {
        None
    } else {
        Some((numerator + denominator - 1) / denominator)
    }
}
pub fn format_label(name: &str) -> String {
    format!("value:{name}")
}
