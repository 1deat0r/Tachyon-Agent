#[derive(Debug)]
pub struct TextLabel {
    text: String,
    label: String,
}
impl TextLabel {
    pub fn new(text: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            label: label.into(),
        }
    }
    pub fn truncate_bytes(&mut self, limit: usize) {
        self.text.truncate(limit);
    }
    pub fn text(&self) -> &str {
        &self.text
    }
    pub fn label(&self) -> &str {
        &self.label
    }
}
