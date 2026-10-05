pub fn midpoint_floor(a: i64, b: i64) -> i64 {
    (a + b).div_euclid(2)
}
pub fn format_label(name: &str) -> String {
    format!("value:{name}")
}
