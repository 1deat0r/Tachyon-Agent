pub fn midpoint_floor(a: i64, b: i64) -> i64 {
    (a & b) + ((a ^ b) >> 1)
}
pub fn format_label(name: &str) -> String {
    format!("value:{name}")
}
