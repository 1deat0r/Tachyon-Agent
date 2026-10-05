pub fn ceil_div(numerator: u64, denominator: u64) -> Option<u64> {
    if denominator == 0 {
        None
    } else {
        Some(numerator / denominator + u64::from(numerator % denominator != 0))
    }
}
pub fn format_label(name: &str) -> String {
    format!("value:{name}")
}
