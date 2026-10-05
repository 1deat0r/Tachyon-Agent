Fix registry insertion and lookup so both canonicalize names by trimming Unicode whitespace and lowercasing ASCII letters only. Reject empty canonical names and duplicate canonical names without modifying existing entries. Preserve non-ASCII letter case, all public APIs, and unrelated Registry methods. Both implementation files need repair.
Only the two declared implementation files may change.
