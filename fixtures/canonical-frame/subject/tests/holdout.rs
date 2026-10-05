use canonical_frame::{FrameError, decode, encode};
#[test]
fn lengths_and_errors_match_independent_wire_oracle() {
    for length in [0usize, 1, 2, 255, 256, 257, 65535] {
        let payload = vec![42; length];
        let mut expected = vec![(length / 256) as u8, (length % 256) as u8];
        expected.extend_from_slice(&payload);
        assert_eq!(encode(&payload).unwrap(), expected);
        assert_eq!(decode(&expected).unwrap(), payload);
        let mut extra = expected.clone();
        extra.push(0);
        assert_eq!(decode(&extra), Err(FrameError::LengthMismatch));
        if length > 0 {
            expected.pop();
            assert_eq!(decode(&expected), Err(FrameError::LengthMismatch));
        }
    }
    assert_eq!(encode(&vec![0; 65536]), Err(FrameError::TooLarge));
    assert_eq!(decode(&[]), Err(FrameError::Truncated));
    assert_eq!(decode(&[0]), Err(FrameError::Truncated));
}
