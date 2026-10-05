use canonical_frame::{decode, encode};
#[test]
fn visible_encoder_golden() {
    assert_eq!(encode(b"abc").unwrap(), vec![0, 3, 97, 98, 99]);
}
#[test]
fn visible_decoder_golden() {
    assert_eq!(decode(&[0, 3, 97, 98, 99]).unwrap(), b"abc");
}
