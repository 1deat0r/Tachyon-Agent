use crate::FrameError;
pub fn decode(frame: &[u8]) -> Result<Vec<u8>, FrameError> {
    if frame.len() < 2 {
        return Err(FrameError::Truncated);
    }
    let length = u16::from_le_bytes([frame[0], frame[1]]) as usize;
    if frame.len() != length + 2 {
        return Err(FrameError::LengthMismatch);
    }
    Ok(frame[2..].to_vec())
}
pub fn decoding_label() -> &'static str {
    "frame-v1"
}
