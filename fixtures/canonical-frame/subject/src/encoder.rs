use crate::FrameError;
pub fn encode(payload: &[u8]) -> Result<Vec<u8>, FrameError> {
    let length = u16::try_from(payload.len()).map_err(|_| FrameError::TooLarge)?;
    let mut frame = length.to_le_bytes().to_vec();
    frame.extend_from_slice(payload);
    Ok(frame)
}
pub fn encoding_label() -> &'static str {
    "frame-v1"
}
