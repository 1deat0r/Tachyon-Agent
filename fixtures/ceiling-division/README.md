Fix ceil_div to return the mathematical ceiling of numerator/denominator for all u64 inputs without overflow. A zero denominator returns None. Preserve the public API and other functions.
Only subject/src/implementation.rs may change.
