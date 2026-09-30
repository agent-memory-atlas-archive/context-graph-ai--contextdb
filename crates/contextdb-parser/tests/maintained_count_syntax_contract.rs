//! The `MAINTAIN COUNT` vocabulary: what a schema author may write, what the
//! parser refuses, and that silence stays silence.
//!
//! A table declares which index-prefix counts the engine keeps current on
//! every write. The declaration is a table-level clause beside `SYNC` and
//! `RETAIN`, changed online by `ALTER TABLE ... SET MAINTAIN COUNT ...`, and
//! each list reuses the `CREATE INDEX` column spelling including the optional
//! per-column `ASC`/`DESC`.

use contextdb_parser::ast::{AlterAction, MaintainedCountsDecl, SortDirection, Statement};
use contextdb_parser::parse;

fn create(sql: &str) -> Option<MaintainedCountsDecl> {
    match parse(sql).unwrap_or_else(|error| panic!("parse `{sql}`: {error}")) {
        Statement::CreateTable(table) => table.maintained_counts,
        other => panic!("`{sql}` parsed as {other:?}"),
    }
}

fn altered(sql: &str) -> MaintainedCountsDecl {
    match parse(sql).unwrap_or_else(|error| panic!("parse `{sql}`: {error}")) {
        Statement::AlterTable(alter) => match alter.action {
            AlterAction::SetMaintainedCounts(declared) => declared,
            other => panic!("`{sql}` parsed as action {other:?}"),
        },
        other => panic!("`{sql}` parsed as {other:?}"),
    }
}

#[test]
fn a_table_that_declares_nothing_carries_no_maintained_count_clause() {
    assert_eq!(
        create("CREATE TABLE events (id UUID PRIMARY KEY, context_id UUID NOT NULL)"),
        None,
        "silence is the default spelling and must not synthesize a clause"
    );
}

#[test]
fn a_declaration_names_each_column_list_in_order() {
    assert_eq!(
        create(
            "CREATE TABLE events (id UUID PRIMARY KEY, context_id UUID NOT NULL, \
             connector_id TEXT NOT NULL, source_instance_ref TEXT NOT NULL) \
             MAINTAIN COUNT (context_id), (context_id, connector_id, source_instance_ref)"
        ),
        Some(MaintainedCountsDecl::Lists(vec![
            vec![("context_id".to_owned(), SortDirection::Asc)],
            vec![
                ("context_id".to_owned(), SortDirection::Asc),
                ("connector_id".to_owned(), SortDirection::Asc),
                ("source_instance_ref".to_owned(), SortDirection::Asc),
            ],
        ]))
    );
}

#[test]
fn a_list_column_carries_the_same_optional_direction_create_index_accepts() {
    assert_eq!(
        create(
            "CREATE TABLE events (id UUID PRIMARY KEY, occurred_at TIMESTAMP) \
             MAINTAIN COUNT (occurred_at DESC)"
        ),
        Some(MaintainedCountsDecl::Lists(vec![vec![(
            "occurred_at".to_owned(),
            SortDirection::Desc
        )]]))
    );
}

#[test]
fn all_prefixes_and_none_are_their_own_spellings() {
    assert_eq!(
        create("CREATE TABLE t (id UUID PRIMARY KEY) MAINTAIN COUNT ALL PREFIXES"),
        Some(MaintainedCountsDecl::AllPrefixes)
    );
    assert_eq!(
        create("CREATE TABLE t (id UUID PRIMARY KEY) MAINTAIN COUNT NONE"),
        Some(MaintainedCountsDecl::None)
    );
}

#[test]
fn the_clause_sits_beside_the_other_table_level_policy_clauses() {
    assert_eq!(
        create(
            "CREATE TABLE t (id UUID PRIMARY KEY, owner TEXT NOT NULL) \
             MAINTAIN COUNT (owner) SYNC OFF SYNC CONFLICT KEEP FIRST"
        ),
        Some(MaintainedCountsDecl::Lists(vec![vec![(
            "owner".to_owned(),
            SortDirection::Asc
        )]]))
    );
}

#[test]
fn one_alter_replaces_the_whole_declared_set() {
    assert_eq!(
        altered("ALTER TABLE events SET MAINTAIN COUNT (context_id)"),
        MaintainedCountsDecl::Lists(vec![vec![("context_id".to_owned(), SortDirection::Asc)]])
    );
    assert_eq!(
        altered("ALTER TABLE events SET MAINTAIN COUNT ALL PREFIXES"),
        MaintainedCountsDecl::AllPrefixes
    );
    assert_eq!(
        altered("ALTER TABLE events SET MAINTAIN COUNT NONE"),
        MaintainedCountsDecl::None
    );
    assert_eq!(
        altered("ALTER TABLE events SET MAINTAIN COUNT DEFAULT"),
        MaintainedCountsDecl::Default,
        "DEFAULT clears the declaration and returns the table to silence"
    );
}

#[test]
fn default_is_refused_on_create_because_a_fresh_table_declares_nothing_to_clear() {
    let error = parse("CREATE TABLE t (id UUID PRIMARY KEY) MAINTAIN COUNT DEFAULT")
        .expect_err("CREATE TABLE ... MAINTAIN COUNT DEFAULT is refused");
    let text = error.to_string();
    assert!(
        text.contains("ALTER TABLE") && text.contains("omit the clause"),
        "the refusal names the statement that does accept it: {text}"
    );
}

#[test]
fn a_repeated_list_a_repeated_column_and_a_repeated_clause_are_refused() {
    for sql in [
        "CREATE TABLE t (id UUID PRIMARY KEY, a TEXT) MAINTAIN COUNT (a), (a)",
        "CREATE TABLE t (id UUID PRIMARY KEY, a TEXT) MAINTAIN COUNT (a, a)",
        "CREATE TABLE t (id UUID PRIMARY KEY, a TEXT) MAINTAIN COUNT (a) MAINTAIN COUNT (a)",
    ] {
        parse(sql).expect_err(&format!("`{sql}` is refused"));
    }
}
