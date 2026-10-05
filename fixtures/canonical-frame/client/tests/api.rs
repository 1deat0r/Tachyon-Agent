use canonical_frame::{FrameError, decode, decoding_label, encode, encoding_label};
#[test]
fn public_api_is_preserved() {
    let _: fn(&[u8]) -> Result<Vec<u8>, FrameError> = encode;
    let _: fn(&[u8]) -> Result<Vec<u8>, FrameError> = decode;
    assert_eq!(encoding_label(), "frame-v1");
    assert_eq!(decoding_label(), "frame-v1");
}
