# duplicate-range

Fix equal_range so it returns the complete half-open range of entries equal to the query in a sorted slice. For absent values return the empty range at the insertion point. Preserve the public API and other functions.

Synthetic eval data. Only subject/src/implementation.rs may change.
The holdout tests are protected and omitted from model evidence.
