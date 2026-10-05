# atomic-transfer

Fix Ledger::transfer so every rejected transfer leaves both balances unchanged. Reject insufficient funds and destination overflow. Preserve the public API and label behavior.

Synthetic eval data. Only subject/src/implementation.rs may change.
The holdout tests are protected and omitted from model evidence.
