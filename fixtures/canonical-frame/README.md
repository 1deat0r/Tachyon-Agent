Fix both encoder and decoder to use the canonical frame-v1 wire format: a two-byte big-endian payload length followed by exactly that many payload bytes. Preserve all public APIs and label functions. Keep TooLarge, Truncated, and LengthMismatch behavior correct. Both implementation files need repair.
Only the two declared implementation files may change.
