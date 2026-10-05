use indexed_catalog::{Catalog, CatalogError};
#[derive(Clone, Copy)]
enum Op {
    Insert(u8, &'static str),
    Rename(u8, &'static str),
    Remove(u8),
}
fn reference(state: &mut Vec<(u8, String)>, op: Op) -> Result<Option<String>, CatalogError> {
    match op {
        Op::Insert(id, name) => {
            if state.iter().any(|(k, _)| *k == id) {
                return Err(CatalogError::DuplicateId);
            }
            if state.iter().any(|(_, v)| v == name) {
                return Err(CatalogError::DuplicateName);
            }
            state.push((id, name.to_string()));
            state.sort_by_key(|(id, _)| *id);
            Ok(None)
        }
        Op::Rename(id, name) => {
            let index = state
                .iter()
                .position(|(k, _)| *k == id)
                .ok_or(CatalogError::UnknownId)?;
            if state.iter().any(|(k, v)| *k != id && v == name) {
                return Err(CatalogError::DuplicateName);
            }
            state[index].1 = name.to_string();
            Ok(None)
        }
        Op::Remove(id) => {
            let index = state
                .iter()
                .position(|(k, _)| *k == id)
                .ok_or(CatalogError::UnknownId)?;
            Ok(Some(state.remove(index).1))
        }
    }
}
fn apply(c: &mut Catalog, op: Op) -> Result<Option<String>, CatalogError> {
    match op {
        Op::Insert(id, name) => c.insert(id, name).map(|()| None),
        Op::Rename(id, name) => c.rename(id, name).map(|()| None),
        Op::Remove(id) => c.remove(id).map(Some),
    }
}
fn verify(c: &Catalog, expected: &[(u8, String)]) {
    let mut reverse: Vec<_> = expected
        .iter()
        .map(|(id, name)| (name.clone(), *id))
        .collect();
    reverse.sort();
    assert_eq!(c.snapshot(), (expected.to_vec(), reverse));
    for id in 0..=3 {
        assert_eq!(
            c.name(id),
            expected
                .iter()
                .find(|(k, _)| *k == id)
                .map(|(_, v)| v.as_str())
        );
    }
    for name in ["a", "b", "c", "", "A", "Ä", "ä"] {
        assert_eq!(
            c.owner(name),
            expected.iter().find(|(_, v)| v == name).map(|(id, _)| *id)
        );
    }
    assert_eq!(c.label(), "catalog-v1");
}
#[test]
fn sequences_match_independent_index_oracle() {
    let ops = [
        Op::Insert(0, "a"),
        Op::Insert(1, "b"),
        Op::Insert(1, "a"),
        Op::Rename(0, "b"),
        Op::Rename(0, "a"),
        Op::Remove(0),
        Op::Remove(1),
        Op::Rename(2, "c"),
        Op::Insert(2, ""),
    ];
    for i in 0..ops.len() {
        for j in 0..ops.len() {
            for k in 0..ops.len() {
                let mut c = Catalog::new();
                let mut expected = Vec::new();
                for index in [i, j, k] {
                    let before = expected.clone();
                    let wanted = reference(&mut expected, ops[index]);
                    assert_eq!(
                        apply(&mut c, ops[index]),
                        wanted,
                        "sequence={i},{j},{k} index={index}"
                    );
                    verify(&c, &expected);
                    if wanted.is_err() {
                        assert_eq!(expected, before);
                    }
                }
            }
        }
    }
}
#[test]
fn exact_names_reuse_and_error_precedence() {
    let mut c = Catalog::new();
    let mut expected = Vec::new();
    for op in [
        Op::Insert(0, "A"),
        Op::Insert(1, "a"),
        Op::Insert(2, "Ä"),
        Op::Insert(3, "ä"),
        Op::Insert(0, "a"),
        Op::Rename(9, "a"),
        Op::Rename(0, "A"),
        Op::Rename(0, "c"),
        Op::Insert(0, "a"),
        Op::Remove(0),
        Op::Insert(0, "A"),
        Op::Remove(1),
        Op::Rename(0, "a"),
        Op::Rename(0, ""),
        Op::Remove(0),
        Op::Insert(0, ""),
    ] {
        let wanted = reference(&mut expected, op);
        assert_eq!(apply(&mut c, op), wanted);
        verify(&c, &expected);
    }
}
