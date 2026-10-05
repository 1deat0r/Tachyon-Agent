use normalized_registry::{RegisterError, Registry};
#[test]
fn canonical_names_errors_and_state_match_literal_oracle() {
    let mut r = Registry::new("original");
    for name in ["", " ", "\t\n", "\u{2003}"] {
        assert_eq!(r.register(name, 99), Err(RegisterError::EmptyName));
        assert_eq!(r.len(), 0);
        assert_eq!(r.lookup(name), None);
    }
    for (name, canonical, value) in [
        (" API ", "api", 1),
        ("\tBeta\n", "beta", 2),
        ("Ä", "Ä", 3),
        ("ä", "ä", 4),
        ("Straße", "straße", 5),
    ] {
        r.register(name, value).unwrap();
        assert_eq!(r.lookup(canonical), Some(value));
        assert_eq!(r.lookup(name), Some(value));
        assert_eq!(r.register(canonical, 100), Err(RegisterError::Duplicate));
        assert_eq!(r.lookup(canonical), Some(value));
    }
    assert_eq!(r.len(), 5);
    assert!(!r.is_empty());
    assert_eq!(r.label(), "original");
    assert_eq!(r.lookup("absent"), None);
}
