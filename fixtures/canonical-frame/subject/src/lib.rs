mod decoder;
mod encoder;
pub use decoder::{decode, decoding_label};
pub use encoder::{encode, encoding_label};
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    TooLarge,
    Truncated,
    LengthMismatch,
}
